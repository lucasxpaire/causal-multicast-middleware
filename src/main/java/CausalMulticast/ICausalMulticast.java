package CausalMulticast;

/**
 * Contrato que a aplicação cliente deve implementar para receber as mensagens entregues pelo middleware.
 */
public interface ICausalMulticast {
    /**
     * Entrega uma mensagem recebida e ordenada causalmente para a aplicação.
     *
     * @param msg Conteúdo textual da mensagem entregue.
     */
    public void deliver(String msg);
}
