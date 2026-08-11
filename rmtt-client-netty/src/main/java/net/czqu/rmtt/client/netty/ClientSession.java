package net.czqu.rmtt.client.netty;

import net.czqu.rmtt.protocol.RmttMessage;

/** Common send/heartbeat surface shared by the TCP/WS and KCP client sessions. */
interface ClientSession {

    long lastSentMs();

    long lastReceivedMs();

    void sendPing();

    void push(RmttMessage msg);
}
