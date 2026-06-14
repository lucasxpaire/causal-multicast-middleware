Especificação do Trabalho II – ELC1018 Sistemas Distribuídos
Programação de um middleware para  
Comunicação multicast com Ordenamento Causal de mensagens e Mecanismo de Estabilização
para descarte de mensagens do buffer baseado em vetor de vetor de relógios lógicos
O trabalho, que deve ser feito em dupla, consiste em implementar um middleware para envio de mensagens multicast
obedecendo à ordem causal e realizando o controle de estabilização de mensagens para descarte das mensagens do
buffer. Consulte o material teórico (no final deste documento) e os slides (no Moodle) para compreender os algoritmos.
O middleware deve ser um único packet Java nomeado CausalMulticast que será importado por algum programa
Java (usuário) e que oferecerá através de sua API a facilidade de enviar uma mensagem multicast para um conjunto de
destinatários (grupo) obedecendo o ordenamento causal. Vide detalhes da API abaixo. O middleware deve implementar a
comunicação multicast através do envio de mensagens unicast não confiável (sockets UDP) e implementar um Serviço
de Descoberta que deve usar IP multicast para realizar a descoberta das instâncias de usuário que executam o middleware
(participantes da computação/grupo). Neste trabalho você não deve usar RMI.
A API do middleware deve oferecer um método mcsend(msg, this), para os usuários enviarem mensagens
multicast com ordenamento causal, e um método deliver(msg), para o usuário receber mensagens. Note que o
parâmetro this do método mcsend é quem passa a referência do objeto remoto do usuário para que a resposta seja
recebida por callback via método deliver. O Serviço de Descoberta deve permanecer sempre ativo em cada instância
do middleware, a fim de permitir atualização dinâmica dos membros do grupo.  
Importação do pacote CausalMulticast:
import CausalMulticast.*;
Interface que deve ser implementada por todo usuário do pacote CausalMulticast:
Public interface ICausalMulticast {
public void deliver(String msg);
}
API oferecida pelo pacote CausalMulticast:
public CausalMulticast(String ip, Integer port, ICausalMulticast client)  
public void mcsend(String msg, ICausalMulticast cliente)
Para possibilitar a correção do trabalho, faça o envio de cada mensagem unicast ser controlado via teclado, ou seja, deve
haver uma pergunta antes de cada envio unicast (controle) questionando se é para enviar a todos ou não. O caso “não”
deve permitir atrasar uma única mensagem escolhida pelo professor/aluno e deve permitir o envio posterior da
mensagem. Por exemplo, com três processos, deve-se poder escolher para qual deles vamos atrasar a mensagem. O
conteúdo do buffer (mensagens) e dos relógios lógicos também precisam ser permanentemente demonstrados na
tela/terminal. Não é necessário implementar uma GUI na aplicação do usuário.
Os algoritmos de ordenação causal e estabilização de mensagens a serem implementados podem ser os propostos no
artigo Fundamentals of Distributed Computing: A Practical Tour of Vector Clock Systems, disponível em Slides no
Moodle e na Web. Porém outros algoritmos com vetores de relógios lógicos também podem ser implementados (indique
isto na documentação, se ocorrer).  
Os algoritmos básicos estão descritos na próxima página.
Na avaliação, o seu middleware será testado com um cliente do professor! Respeite as especificações!  
A entrega do trabalho deve ser no Moodle e deve inclui código e documentação no formato Oxigen ou JavaDoc.

procedure mcsend(msg)          
    msg.VC  VCi                                             
    x  {1,...,n} do send(msg) to Px enddo                   
    VCi[i]  VCi[i]+1                                        

when Pi receives msg from Pj                               
    atrasa entrega até (x  {1,...,n} : msg.VC[x] ≤ VCi[x])    
    if i ≠ j then VCi[j]  VCi[j]+1  
    entrega msg para a aplicação       

(figura1.png)

procedure mcsend(msg)               
    msg.VC  MCi[i][*]                         
    msg.sender  i
    for all P do send(msg) to Pj enddo    
    MCi[i][i]  MCi[i][i]+1

when Pi receives msg from Pj            
    deposit(msg)                                
    MCi[j][*]  msg.VC                   
    if i ≠ j then MCi[i][j]  MCi[i][j]+1  
    deliver msg to the upper layer  

when (existe msg no bufferi AND msg.VC[msg.sender] ≤ min1≤x≤n(MCi[x][msg.sender])
discart(msg)  

(figura2.png)