// Clase que implementa la interfaz remota Subscriber
package broker;
import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import pubsub.Event;

class SubscriberImpl extends UnicastRemoteObject implements Subscriber  {
    public static final long serialVersionUID=1234567890L;
    UUID subUUID; // para facilitar depuración
    PubSubImpl ps; // para acceder a funcionalidad del servicio general
    // para notificar al subscriptor de creación y destrucción de temas
    transient SubscriberCallback scbk; 
    private   List <String> SuscripcionesList;
    private  BlockingQueue <Event> EventosCola;

    


    public SubscriberImpl(PubSubImpl p, SubscriberCallback s) throws RemoteException {
        
        scbk=s;
        subUUID = UUID.randomUUID();
	    ps=p;
        SuscripcionesList = new ArrayList<>();
        EventosCola = new ArrayBlockingQueue <>(100,true);
    }
    public UUID getUUID() throws RemoteException {
        return subUUID;
    }
    public int subscribe(String topic, boolean glob) throws RemoteException {
         if(!ps.topicList().contains(topic)){
            return 0;
        
    }
        if (SuscripcionesList.contains(topic)){
        return 0;
    }
        SuscripcionesList.add(topic);
   
        
        return 1;
    }
    public Event getEvent() throws RemoteException {

        try{
       return  EventosCola.poll(); }
        catch(Exception e){

                throw new RemoteException("error cola ,e");

        }
            
       
    }
    public Collection<String> topicListBySubscriber() throws RemoteException {
        return SuscripcionesList;
    }
    public boolean unsubscribe(String topic) throws RemoteException {
        return true;
    }
    public void exit() throws RemoteException {
    }
    public void NotificaCreacion(String Topic) throws RemoteException{
        if (scbk != null){
        scbk.topicAdded(Topic);}
    }

    public void NotificaDestruccion(String Topic) throws RemoteException{
         if (scbk != null){
        
         scbk.topicRemoved(Topic);}
    }
    public void EnviarEvento(Event ev) throws RemoteException{
        if (SuscripcionesList.contains(ev.getTopic())){
            try{
        EventosCola.put(ev); }
        catch(Exception e){

                throw new RemoteException("error cola ,e");

        }
            
        }
    }
}
       

    
    

