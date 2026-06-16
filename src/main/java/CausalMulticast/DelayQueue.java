package CausalMulticast;

import java.util.concurrent.*;

/**
 * Construtor da fila de atrasos.
 */
public class DelayQueue {

    /** Mapa concorrente que associa cada identificador de peer à sua respectiva fila de agendamento de tarefas. */
    private final ConcurrentHashMap<String, DelayedMessageQueue> peerQueues = new ConcurrentHashMap<>();

    /** Referência do middleware de ordenação causal para onde as mensagens serão despachadas após o término do atraso. */
    private final CausalMulticast causalMulticast;

    /**
     * Construtor do gerenciador de filas de atraso.
     *  @param causalMulticast Instância do middleware de ordenação causal associada ao nó local.
     */
    public DelayQueue(CausalMulticast causalMulticast) {
        this.causalMulticast = causalMulticast;
    }

    /**
     * Adiciona uma mensagem à fila de atraso de um peer.
     *  @param peerId ID do peer destinatário.
     * @param message Mensagem a atrasar.
     * @param delayMillis Tempo de atraso em milissegundos.
     */
    public void addDelayedMessage(String peerId, BufferedMessage message, long delayMillis) {
        peerQueues.computeIfAbsent(peerId, k -> new DelayedMessageQueue())
                .addMessage(message, delayMillis);
    }

    /**
     * Define o atraso padrão para um peer específico.
     *  @param peerId ID do peer alvo.
     * @param delayMillis Tempo de retenção em milissegundos.
     */
    public void setPeerDelay(String peerId, long delayMillis) {
        peerQueues.computeIfAbsent(peerId, k -> new DelayedMessageQueue())
                .setDefaultDelay(delayMillis);
    }

    /**
     * Retorna o atraso configurado para um peer.
     *  @param peerId ID do peer que se deseja consultar.
     * @return O atraso padrão mapeado para este peer em milissegundos.
     */
    public long getPeerDelay(String peerId) {
        DelayedMessageQueue queue = peerQueues.get(peerId);
        return queue != null ? queue.getDefaultDelay() : 0;
    }

    /**
     * Classe interna responsável por gerenciar o agendamento de tarefas e o
     * ciclo de vida do executor thread-pool de um peer individual.
     */
    private class DelayedMessageQueue {

        /** Tempo de atraso padrão (em milissegundos) aplicado às mensagens deste peer específico. */
        private long defaultDelay = 0;

        /** Fila thread-safe contendo as referências das tarefas futuras de agendamento que ainda não foram executadas ou canceladas. */
        private final java.util.Queue<ScheduledFuture<?>> pendingTasks = new ConcurrentLinkedQueue<>();

        /** Agendador interno de thread única dedicado a disparar os eventos de liberação de mensagens deste peer. */
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        /**
         * Cria e agenda uma tarefa assíncrona para liberar a mensagem de volta ao middleware
         * após o estouro do cronômetro de atraso.
         *  @param message Mensagem que será retida temporariamente.
         * @param delayMillis Tempo de agendamento em milissegundos.
         */
        public void addMessage(BufferedMessage message, long delayMillis) {
            ScheduledFuture<?> task = scheduler.schedule(() -> {
                causalMulticast.onMessageReceived(message);
                System.out.println("[DELAY QUEUE] Mensagem liberada após " + delayMillis + "ms");
            }, delayMillis, TimeUnit.MILLISECONDS);

            pendingTasks.offer(task);
        }

        /**
         * Modifica o valor do atraso padrão deste canal de comunicação.
         *  @param delayMillis Novo valor em milissegundos.
         */
        public void setDefaultDelay(long delayMillis) {
            this.defaultDelay = delayMillis;
        }

        /**
         * Obtém o valor do atraso padrão deste canal de comunicação.
         *  @return O atraso configurado em milissegundos.
         */
        public long getDefaultDelay() {
            return defaultDelay;
        }

        /**
         * Encerra as atividades do agendador deste peer de forma limpa.
         * Cancela preventivamente todas as mensagens que estavam retidas e aguardando liberação
         * e desativa o pool de execução associado.
         */
        public void shutdown() {
            for (ScheduledFuture<?> task : pendingTasks) {
                task.cancel(false);
            }
            scheduler.shutdown();
        }
    }

    /**
     * Realiza o desligamento em cascata de todas as filas de atraso ativas no sistema.
     * Deve ser invocado no encerramento da aplicação para liberar recursos de threads abertas.
     */
    public void shutdown() {
        peerQueues.values().forEach(DelayedMessageQueue::shutdown);
    }
}