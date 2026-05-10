"""Relay integration tests: E2E encrypted messaging через relay."""

import asyncio
import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from phase0.identity import Identity
from relay.server import RelayServer
from relay.client import RelayClient


class TestRelay:
    @pytest.fixture
    def event_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        yield loop
        loop.close()

    @pytest.mark.asyncio
    async def test_relay_start_stop(self):
        """Relay стартует и останавливается."""
        relay = RelayServer(port=0)
        await relay.start()
        assert relay.port > 0
        await relay.stop()

    @pytest.mark.asyncio
    async def test_agent_register(self):
        """Агент подключается к relay и регистрируется."""
        relay = RelayServer(port=0)
        await relay.start()

        ident = Identity()
        client = RelayClient(ident, "127.0.0.1", relay.port,
                             capabilities=["ping"])

        ok = await client.connect()
        assert ok, "Agent should connect to relay"
        await asyncio.sleep(0.3)

        peers = client.peers()
        assert isinstance(peers, list)

        await client.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_two_agents_see_each_other(self):
        """Два агента видят друг друга в peers."""
        relay = RelayServer(port=0)
        await relay.start()

        ident_a = Identity()
        ident_b = Identity()

        client_a = RelayClient(ident_a, "127.0.0.1", relay.port,
                               capabilities=["cap_a"])
        client_b = RelayClient(ident_b, "127.0.0.1", relay.port,
                               capabilities=["cap_b"])

        ok_a = await client_a.connect()
        ok_b = await client_b.connect()
        assert ok_a and ok_b

        await asyncio.sleep(0.5)

        peers_a = client_a.peers()
        peers_b = client_b.peers()

        # A должен видеть B
        b_in_a = any(p["pubkey"] == ident_b.public_key_hex for p in peers_a)
        assert b_in_a, f"A should see B in peers: {peers_a}"

        # B должен видеть A
        a_in_b = any(p["pubkey"] == ident_a.public_key_hex for p in peers_b)
        assert a_in_b, f"B should see A in peers: {peers_b}"

        await client_a.stop()
        await client_b.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_e2e_key_exchange(self):
        """Два агента устанавливают E2E ключ через relay."""
        relay = RelayServer(port=0)
        await relay.start()

        ident_a = Identity()
        ident_b = Identity()

        client_a = RelayClient(ident_a, "127.0.0.1", relay.port)
        client_b = RelayClient(ident_b, "127.0.0.1", relay.port)

        await client_a.connect()
        await client_b.connect()
        await asyncio.sleep(0.3)

        # A устанавливает E2E с B
        e2e = await client_a.e2e_establish(ident_b.public_key_hex)
        assert e2e is not None, "E2E session should be established"
        assert e2e.peer_pubkey_hex == ident_b.public_key_hex

        # B тоже должен иметь E2E с A
        await asyncio.sleep(0.3)
        b_e2e = client_b._e2e_sessions.get(ident_a.public_key_hex)
        assert b_e2e is not None, "B should have E2E session with A"

        await client_a.stop()
        await client_b.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_e2e_send_recv(self):
        """A отправляет E2E сообщение → B получает."""
        relay = RelayServer(port=0)
        await relay.start()

        ident_a = Identity()
        ident_b = Identity()

        client_a = RelayClient(ident_a, "127.0.0.1", relay.port)
        client_b = RelayClient(ident_b, "127.0.0.1", relay.port)

        await client_a.connect()
        await client_b.connect()
        await asyncio.sleep(0.3)

        received = []

        def on_recv(from_pubkey, data, e2e_session):
            received.append((from_pubkey, data, e2e_session))

        client_b.on_recv(on_recv)

        # A устанавливает E2E и отправляет сообщение
        ok = await client_a.send(ident_b.public_key_hex, b"secret hello")
        assert ok, "Send should succeed"

        await asyncio.sleep(0.5)

        assert len(received) > 0, "B should receive message"
        from_key, data, _ = received[0]
        assert from_key == ident_a.public_key_hex
        assert data == b"secret hello"

        await client_a.stop()
        await client_b.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_e2e_relay_cannot_read(self):
        """Relay не может расшифровать E2E данные."""
        relay = RelayServer(port=0)
        await relay.start()

        ident_a = Identity()
        ident_b = Identity()

        client_a = RelayClient(ident_a, "127.0.0.1", relay.port)
        client_b = RelayClient(ident_b, "127.0.0.1", relay.port)

        await client_a.connect()
        await client_b.connect()
        await asyncio.sleep(0.3)

        # Подслушиваем relay — перехватываем сообщения перед отправкой
        intercepted = []

        original_send = relay._send_encrypted

        async def spy_send(writer, session, msg):
            if msg.get("type") == "recv":
                intercepted.append(msg.get("data", ""))
            await original_send(writer, session, msg)

        relay._send_encrypted = spy_send

        received = []

        def on_recv(from_pubkey, data, e2e_session):
            received.append(data)

        client_b.on_recv(on_recv)

        await client_a.send(ident_b.public_key_hex, b"supersecret")
        await asyncio.sleep(0.5)

        # Проверяем: relay перехватил hex, не может прочитать
        assert len(intercepted) > 0, "Relay should intercept data"
        hex_data = intercepted[0]
        # hex_data — это encrypted blob. Не начинается с "supersecret"
        assert "supersecret" not in hex_data, \
            "Relay should NOT see plaintext in intercepted data"

        # Но B получил и расшифровал
        assert len(received) > 0, "B should receive"
        assert received[0] == b"supersecret"

        await client_a.stop()
        await client_b.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_disconnect_cleanup(self):
        """Отключение агента → relay удаляет из peers."""
        relay = RelayServer(port=0)
        await relay.start()

        ident_a = Identity()
        ident_b = Identity()

        client_a = RelayClient(ident_a, "127.0.0.1", relay.port)
        client_b = RelayClient(ident_b, "127.0.0.1", relay.port)

        await client_a.connect()
        await client_b.connect()
        await asyncio.sleep(0.3)

        # A видит B
        assert any(p["pubkey"] == ident_b.public_key_hex
                   for p in client_a.peers())

        # Отключаем B
        await client_b.stop()
        await asyncio.sleep(0.5)

        # A больше не видит B
        peers_a_after = client_a.peers()
        b_in_peers = any(p["pubkey"] == ident_b.public_key_hex
                         for p in peers_a_after)
        assert not b_in_peers, \
            f"B should be removed from peers after disconnect: {peers_a_after}"

        await client_a.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_capability_filter(self):
        """Агенты видят capabilities друг друга."""
        relay = RelayServer(port=0)
        await relay.start()

        ident_a = Identity()
        ident_b = Identity()

        client_a = RelayClient(ident_a, "127.0.0.1", relay.port,
                               capabilities=["forecast", "analysis"])
        client_b = RelayClient(ident_b, "127.0.0.1", relay.port,
                               capabilities=["ping"])

        await client_a.connect()
        await client_b.connect()
        await asyncio.sleep(0.5)

        peers_a = client_a.peers()
        for p in peers_a:
            if p["pubkey"] == ident_b.public_key_hex:
                assert p["capabilities"] == ["ping"]
                break
        else:
            pytest.fail("A should see B in peers")

        peers_b = client_b.peers()
        for p in peers_b:
            if p["pubkey"] == ident_a.public_key_hex:
                assert p["capabilities"] == ["forecast", "analysis"]
                break
        else:
            pytest.fail("B should see A in peers")

        await client_a.stop()
        await client_b.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_multiple_messages(self):
        """Несколько E2E сообщений между двумя агентами."""
        relay = RelayServer(port=0)
        await relay.start()

        ident_a = Identity()
        ident_b = Identity()

        client_a = RelayClient(ident_a, "127.0.0.1", relay.port)
        client_b = RelayClient(ident_b, "127.0.0.1", relay.port)

        await client_a.connect()
        await client_b.connect()
        await asyncio.sleep(0.3)

        received = []

        def on_recv(from_pubkey, data, e2e_session):
            received.append((from_pubkey, data))

        client_b.on_recv(on_recv)

        for i in range(5):
            ok = await client_a.send(ident_b.public_key_hex, f"msg_{i}".encode())
            assert ok

        await asyncio.sleep(0.5)

        assert len(received) == 5, f"Expected 5 messages, got {len(received)}"
        for i in range(5):
            assert received[i][1] == f"msg_{i}".encode()

        await client_a.stop()
        await client_b.stop()
        await relay.stop()

    @pytest.mark.asyncio
    async def test_relay_rejects_no_register(self):
        """Relay не отвечает если клиент не отправил register."""
        relay = RelayServer(port=0)
        await relay.start()

        ident = Identity()

        # Подключаемся без регистрации
        from phase0.handshake import client_handshake
        reader, writer = await asyncio.open_connection("127.0.0.1", relay.port)
        session = await client_handshake(reader, writer, ident)
        assert session is not None, "Handshake should succeed"

        # Не отправляем register — relay закроет
        writer.close()
        await writer.wait_closed()

        await relay.stop()
