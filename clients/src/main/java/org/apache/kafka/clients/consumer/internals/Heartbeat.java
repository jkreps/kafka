package org.apache.kafka.clients.consumer.internals;

/**
 * A helper class for managing the heartbeat to the co-ordinator
 */
public final class Heartbeat {

    private final long timeout;
    private long lastHeartbeatSend;
    private long lastHeartbeatResponse;

    public Heartbeat(long timeout, long now) {
        this.timeout = timeout;
        this.lastHeartbeatSend = now;
        this.lastHeartbeatResponse = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
    }

    public void receivedResponse(long now) {
        this.lastHeartbeatResponse = now;
    }

    public void markDead() {
        this.lastHeartbeatResponse = -1;
    }

    public boolean isAlive(long now) {
        return now - lastHeartbeatResponse <= timeout;
    }

    public boolean shouldHeartbeat(long now) {
        return now - lastHeartbeatSend > 0.3 * this.timeout;
    }
}