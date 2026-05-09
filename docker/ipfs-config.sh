#!/bin/bash
# Настройка IPFS для pubsub + DHT в Docker
ipfs config --json Experimental.Libp2pStreamMounting true
ipfs config --json Experimental.P2pHttpProxy true
ipfs config --json Swarm.EnableRelayHop true
ipfs config --json Discovery.MDNS.Enabled true
ipfs config --json Routing.Type "dht"
ipfs config --json Swarm.DisableNatPortMap false
echo "IPFS configured for pubsub + DHT"
