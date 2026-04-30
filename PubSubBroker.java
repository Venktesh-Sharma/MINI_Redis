package com.miniredis.pubsub;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Pub/Sub broker.
 *
 * Subscribers register a Listener callback against a channel name.
 * Publishers push messages to a channel; all subscribers receive them.
 *
 * Design: ConcurrentHashMap<channel, CopyOnWriteArrayList<Listener>>
 *   - ConcurrentHashMap: safe concurrent channel lookup
 *   - CopyOnWriteArrayList: safe iteration while publish is in-flight
 */
public class PubSubBroker {

    /**
     * Callback interface delivered to each subscriber when a message arrives.
     */
    @FunctionalInterface
    public interface Listener {
        void onMessage(String channel, String message);
    }

    // channel → list of subscribers
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Listener>> channels =
        new ConcurrentHashMap<>();

    // subscriber id → subscribed channels (for UNSUBSCRIBE ALL)
    private final ConcurrentHashMap<String, Set<String>> subscriberChannels =
        new ConcurrentHashMap<>();

    /**
     * Subscribe a listener to a channel. Returns current subscriber count for the channel.
     */
    public int subscribe(String channel, String subscriberId, Listener listener) {
        channels.computeIfAbsent(channel, k -> new CopyOnWriteArrayList<>()).add(listener);
        subscriberChannels.computeIfAbsent(subscriberId, k -> ConcurrentHashMap.newKeySet()).add(channel);
        return channels.get(channel).size();
    }

    /**
     * Unsubscribe a listener from a channel. Returns remaining subscriber count.
     */
    public int unsubscribe(String channel, String subscriberId, Listener listener) {
        CopyOnWriteArrayList<Listener> listeners = channels.get(channel);
        if (listeners != null) {
            listeners.remove(listener);
            if (listeners.isEmpty()) channels.remove(channel);
        }
        Set<String> subs = subscriberChannels.get(subscriberId);
        if (subs != null) subs.remove(channel);
        return listeners != null ? listeners.size() : 0;
    }

    /**
     * Publish a message to a channel. Returns number of subscribers that received it.
     */
    public int publish(String channel, String message) {
        CopyOnWriteArrayList<Listener> listeners = channels.get(channel);
        if (listeners == null || listeners.isEmpty()) return 0;

        int count = 0;
        for (Listener listener : listeners) {
            try {
                listener.onMessage(channel, message);
                count++;
            } catch (Exception e) {
                System.err.println("[PubSub] Listener error on channel " + channel + ": " + e.getMessage());
            }
        }
        return count;
    }

    /**
     * Returns all active channel names.
     */
    public Set<String> activeChannels() {
        return Collections.unmodifiableSet(channels.keySet());
    }

    /**
     * Returns subscriber count for a given channel.
     */
    public int subscriberCount(String channel) {
        CopyOnWriteArrayList<Listener> listeners = channels.get(channel);
        return listeners != null ? listeners.size() : 0;
    }

    /**
     * Returns all channels a subscriber is subscribed to.
     */
    public Set<String> channelsForSubscriber(String subscriberId) {
        return subscriberChannels.getOrDefault(subscriberId, Collections.emptySet());
    }
}
