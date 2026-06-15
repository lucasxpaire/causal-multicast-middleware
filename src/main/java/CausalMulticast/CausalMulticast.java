package CausalMulticast;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class CausalMulticast {

    private final String localId;
    private final ICausalMulticast client;
    private final List<String> activePeers;
    private Map<String, Integer> peerToIndex;
    private int[][] matrixClock;
    private final List<BufferedMessage> messageBuffer;

    public CausalMulticast(String ip, Integer port, ICausalMulticast client) {
        this.localId = ip + ":" + port;
        this.client = client;
        this.activePeers = new CopyOnWriteArrayList<>();
        this.peerToIndex = new ConcurrentHashMap<>();
        this.messageBuffer = Collections.synchronizedList(new ArrayList<BufferedMessage>());
        this.updateGroupMembers(List.of(this.localId));
    }

    public synchronized void updateGroupMembers(List<String> newPeers) {
        List<String> sortedPeers = new ArrayList<>(newPeers);
        if (!sortedPeers.contains(localId)) {
            sortedPeers.add(localId);
        }
        Collections.sort(sortedPeers);

        Map<String, Integer> oldPeerToIndex = this.peerToIndex;
        int[][] oldMatrixClock = this.matrixClock;

        this.activePeers.clear();
        this.activePeers.addAll(sortedPeers);

        int newSize = sortedPeers.size();
        Map<String, Integer> newPeerToIndex = new ConcurrentHashMap<>();
        for (int i = 0; i < newSize; i++) {
            newPeerToIndex.put(sortedPeers.get(i), i);
        }
        this.peerToIndex = newPeerToIndex;

        int[][] newMatrixClock = new int[newSize][newSize];

        if (oldMatrixClock != null && oldPeerToIndex != null) {
            for (String rowPeer : oldPeerToIndex.keySet()) {
                if (newPeerToIndex.containsKey(rowPeer)) {
                    int oldRowIdx = oldPeerToIndex.get(rowPeer);
                    int newRowIdx = newPeerToIndex.get(rowPeer);
                    for (String colPeer : oldPeerToIndex.keySet()) {
                        if (newPeerToIndex.containsKey(colPeer)) {
                            int oldColIdx = oldPeerToIndex.get(colPeer);
                            int newColIdx = newPeerToIndex.get(colPeer);
                            newMatrixClock[newRowIdx][newColIdx] = oldMatrixClock[oldRowIdx][oldColIdx];
                        }
                    }
                }
            }
        }
        this.matrixClock = newMatrixClock;
    }

    public void mcsend(String msg, ICausalMulticast cliente) {
        Integer localPeerIdx = this.peerToIndex.get(this.localId);
        if (localPeerIdx == null) {
            return;
        }

        Map<String, Integer> currentVectorClock = new ConcurrentHashMap<>();
        synchronized (this) {
            for (String peer : this.activePeers) {
                Integer peerIdx = this.peerToIndex.get(peer);
                if (peerIdx != null) {
                    currentVectorClock.put(peer, this.matrixClock[localPeerIdx][peerIdx]);
                }
            }
        }

        BufferedMessage message = new BufferedMessage(msg, this.localId, currentVectorClock);
        sendToGroup(message);

        synchronized (this) {
            localPeerIdx = this.peerToIndex.get(this.localId);
            if (localPeerIdx != null) {
                this.matrixClock[localPeerIdx][localPeerIdx] = this.matrixClock[localPeerIdx][localPeerIdx] + 1;
            }
        }
    }

    private void sendToGroup(BufferedMessage message) {
        // TODO: implementar a transmissão UDP
    }
}
