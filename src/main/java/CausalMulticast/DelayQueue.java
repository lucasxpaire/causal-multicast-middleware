package CausalMulticast;

import java.util.concurrent.*;

public class DelayQueue {
    private final ConcurrentHashMap<String, DelayedMessageQueue> peerQueues = new ConcurrentHashMap<>();
    private final CausalMulticast causalMulticast;

    public DelayQueue(CausalMulticast causalMulticast) {
        this.causalMulticast = causalMulticast;
    }

    /**
     * Adiciona uma mensagem à fila de atraso de um peer
     * @param peerId ID do peer destinatário
     * @param message Mensagem a atrasar
     * @param delayMillis Tempo de atraso em milissegundos
     */
    public void addDelayedMessage(String peerId, BufferedMessage message, long delayMillis) {
        peerQueues.computeIfAbsent(peerId, k -> new DelayedMessageQueue())
                .addMessage(message, delayMillis);
    }

    /**
     * Define o atraso padrão para um peer específico
     */
    public void setPeerDelay(String peerId, long delayMillis) {
        peerQueues.computeIfAbsent(peerId, k -> new DelayedMessageQueue())
                .setDefaultDelay(delayMillis);
    }

    /**
     * Retorna o atraso configurado para um peer
     */
    public long getPeerDelay(String peerId) {
        DelayedMessageQueue queue = peerQueues.get(peerId);
        return queue != null ? queue.getDefaultDelay() : 0;
    }

    // Classe interna para gerenciar fila de atraso por peer
    private class DelayedMessageQueue {
        private long defaultDelay = 0;
        private final java.util.Queue<ScheduledFuture<?>> pendingTasks = new ConcurrentLinkedQueue<>();
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        public void addMessage(BufferedMessage message, long delayMillis) {
            ScheduledFuture<?> task = scheduler.schedule(() -> {
                causalMulticast.onMessageReceived(message);
                System.out.println("[DELAY QUEUE] Mensagem liberada após " + delayMillis + "ms");
            }, delayMillis, TimeUnit.MILLISECONDS);

            pendingTasks.offer(task);
        }

        public void setDefaultDelay(long delayMillis) {
            this.defaultDelay = delayMillis;
        }

        public long getDefaultDelay() {
            return defaultDelay;
        }

        public void shutdown() {
            for (ScheduledFuture<?> task : pendingTasks) {
                task.cancel(false);
            }
            scheduler.shutdown();
        }
    }

    public void shutdown() {
        peerQueues.values().forEach(DelayedMessageQueue::shutdown);
    }
}
