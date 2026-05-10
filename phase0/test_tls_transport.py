"""Phase 0 — TLS Transport Tests: handshake + encrypted pub/sub."""

import asyncio
import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from phase0.identity import Identity
from phase0.handshake import (
    SecureSession, server_handshake, client_handshake,
    is_encrypted_envelope,
)
from phase0.transport import P2PTransport


# ─────────────────────────────────────────────
# Unit: SecureSession encrypt/decrypt roundtrip
# ─────────────────────────────────────────────

class TestSecureSession:
    def test_encrypt_decrypt_roundtrip(self):
        key = b"\x01" * 32
        s1 = SecureSession(key)
        s2 = SecureSession(key)
        # s1 отправляет, s2 получает (разные seq)
        s2._recv_seq = 0  # первый пакет
        plaintext = b"hello mesh!"
        ct = s1.encrypt(plaintext)
        assert ct[:12] == b"\x00" * 4 + b"\x00" * 8  # nonce seq=0
        decrypted = s2.decrypt(ct)
        assert decrypted == plaintext

    def test_encrypt_json_envelope(self):
        key = b"\x02" * 32
        session = SecureSession(key)
        msg = {"type": "pub", "topic": "test", "data": "aGVsbG8="}
        envelope = session.pack_encrypted(msg)
        assert envelope["type"] == "enc"
        assert "d" in envelope
        assert len(envelope["d"]) > 0

        # Распаковка
        unpacked = session.unpack_encrypted(envelope)
        assert unpacked == msg

    def test_multiple_messages_ordered(self):
        key = b"\x03" * 32
        s1 = SecureSession(key)
        s2 = SecureSession(key)
        for i in range(5):
            ct = s1.encrypt(f"msg_{i}".encode())
            dec = s2.decrypt(ct)
            assert dec.decode() == f"msg_{i}"


# ─────────────────────────────────────────────
# Unit: Handshake protocol
# ─────────────────────────────────────────────

class TestHandshake:
    @pytest.mark.asyncio
    async def test_handshake_loopback(self):
        """Полный handshake через loopback TCP."""
        server_ident = Identity()
        client_ident = Identity()

        # Server
        server_ready = asyncio.Event()
        server_result = []

        async def server_task():
            async def handler(reader, writer):
                session = await server_handshake(reader, writer, server_ident)
                server_result.append(session)
                server_ready.set()
                # Не закрываем — тест ниже использует соединение

            server = await asyncio.start_server(handler, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            async with server:
                await asyncio.sleep(0.1)  # даём серверу стартовать
                # Client
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                session = await client_handshake(
                    reader, writer, client_ident
                )
                assert session is not None, "Client handshake failed"
                assert session.peer_pubkey_hex == server_ident.public_key_hex
                writer.close()
                await writer.wait_closed()
                # Ждём серверный handshake
                await asyncio.wait_for(server_ready.wait(), 5.0)
                assert len(server_result) > 0
                srv = server_result[0]
                assert srv is not None, "Server handshake failed"
                assert srv.peer_pubkey_hex == client_ident.public_key_hex

        await asyncio.wait_for(server_task(), 10.0)

    @pytest.mark.asyncio
    async def test_handshake_reject_wrong_pubkey(self):
        """Client с wrong pubkey — handshake проваливается на верификации."""
        server_ident = Identity()
        client_ident = Identity()

        async def server_task():
            server_ready = asyncio.Event()
            server_session = [None]

            async def handler(reader, writer):
                session = await server_handshake(reader, writer, server_ident)
                server_session[0] = session
                server_ready.set()

            server = await asyncio.start_server(handler, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            async with server:
                await asyncio.sleep(0.1)
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                # Клиент с неправильным pubkey (не соответствует приватному ключу)
                session = await client_handshake(
                    reader, writer, client_ident,
                    expected_server_pubkey="00" * 32  # неверный pubkey сервера
                )
                assert session is None, "Ручкопожатие с неверным pubkey не должно пройти"
                writer.close()
                await writer.wait_closed()
                await asyncio.wait_for(server_ready.wait(), 3.0)
                # На сервере тоже None (клиент не прошёл верификацию)
                # или сервер получил hello но auth не прошёл

        await asyncio.wait_for(server_task(), 10.0)


# ─────────────────────────────────────────────
# Integration: TLS transport end-to-end
# ─────────────────────────────────────────────

class TestTLSTransport:
    @pytest.mark.asyncio
    async def test_tls_pub_sub(self):
        """Два транспорта с TLS: A publish → B listen получает."""
        ident_a = Identity()
        ident_b = Identity()

        transport_a = P2PTransport(node_id="tls_a", identity=ident_a, use_tls=True)
        transport_b = P2PTransport(node_id="tls_b", identity=ident_b, use_tls=True)

        received = []

        try:
            pid_a = await transport_a.start("127.0.0.1", 0)
            port_a = transport_a._tcp_port

            pid_b = await transport_b.start("127.0.0.1", 0)

            # B подключается к A
            transport_b._bootstrap_peers = [f"{ident_a.public_key_hex}@127.0.0.1:{port_a}"]
            transport_b._start_reconnect_loop(
                ident_a.public_key_hex, "127.0.0.1", port_a
            )

            await asyncio.sleep(1.5)  # ждём handshake + reconnect

            def cb(data):
                received.append(data)

            await transport_b.subscribe("test_tls", cb)
            await asyncio.sleep(0.5)

            # A публикует
            await transport_a.publish("test_tls", b"secret msg")
            await asyncio.sleep(1.0)

            assert len(received) > 0, "B не получил сообщение по TLS"
            msg = received[0]
            assert b"secret msg" in msg

        finally:
            await transport_a.stop()
            await transport_b.stop()

    @pytest.mark.asyncio
    async def test_tls_reject_wrong_identity(self):
        """Клиент с wrong pubkey → handshake не проходит."""
        # Пробуем подключиться с неверным ожидаемым pubkey сервера
        ident = Identity()
        transport_a = P2PTransport(node_id="tls_server", identity=ident, use_tls=True)

        try:
            pid_a = await transport_a.start("127.0.0.1", 0)
            port_a = transport_a._tcp_port

            # Клиент подключается с неверным pubkey (другой identity)
            wrong_ident = Identity()
            reader, writer = await asyncio.open_connection("127.0.0.1", port_a)
            from phase0.handshake import client_handshake
            session = await client_handshake(
                reader, writer, wrong_ident,
                expected_server_pubkey="00" * 32  # false
            )
            assert session is None, "Handshake с неверным pubkey должен провалиться"
            writer.close()
            await writer.wait_closed()

        finally:
            await transport_a.stop()


# ─────────────────────────────────────────────
# Backward compatibility: MESH_TLS=0
# ─────────────────────────────────────────────

class TestBackwardCompat:
    @pytest.mark.asyncio
    async def test_plain_tcp_still_works(self):
        """MESH_TLS=0: обычный pub/sub как в v0.3.1."""
        from phase0.transport import P2PTransport

        transport_a = P2PTransport(node_id="plain_a", use_tls=False)
        transport_b = P2PTransport(node_id="plain_b", use_tls=False)

        received = []

        try:
            pid_a = await transport_a.start("127.0.0.1", 0)
            port_a = transport_a._tcp_port

            pid_b = await transport_b.start("127.0.0.1", 0)

            # Подключаем B к A
            transport_b._bootstrap_peers = [f"node_plain_a@127.0.0.1:{port_a}"]
            transport_b._start_reconnect_loop("node_plain_a", "127.0.0.1", port_a)
            await asyncio.sleep(0.5)

            def cb(data):
                received.append(data)

            await transport_b.subscribe("plain_test", cb)
            await asyncio.sleep(0.3)

            await transport_a.publish("plain_test", b"hello plain")
            await asyncio.sleep(0.5)

            assert len(received) > 0, "Plain TCP не работает"
            assert b"hello plain" in received[0]

        finally:
            await transport_a.stop()
            await transport_b.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "--timeout=30"])
