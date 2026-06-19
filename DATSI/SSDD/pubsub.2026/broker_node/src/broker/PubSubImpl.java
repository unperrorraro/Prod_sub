// Servidor que implementa la interfaz remota PubSub
package broker;



import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import pubsub.Event;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Flow;


class PubSubImpl extends UnicastRemoteObject implements PubSub  {
    public static final long serialVersionUID=1234567890L;
    private static Map <String,Topic> TemasMap ;
    private static List <Subscriber> SubsList ;
    public PubSubImpl() throws RemoteException {
        TemasMap = new HashMap <String,Topic> ();
        
        SubsList = new ArrayList<Subscriber>();
    }
    public int getVersion() throws RemoteException { // ya programada
        return version;
    }
    public synchronized boolean createTopic(String topic) throws RemoteException {

        
        if(TemasMap.containsKey(topic)){
        return false;}
        else{
            Topic t = new Topic();
            TemasMap.put(topic, t);
            for (Subscriber subscriber : SubsList) {
               SubscriberImpl S = (SubscriberImpl) subscriber;
               S.NotificaCreacion(topic);
                
            }
            return true;
        }
    }
    public synchronized Collection<String> topicList() throws RemoteException {
    
        try {
                        
            Set <String> H = new HashSet<>(TemasMap.keySet());
            return H;
            
        } catch (Exception e) {
            System.err.println("error");
            throw e;
        }
       
        
    }
    public synchronized boolean publish(Event ev) throws RemoteException {
        try {
            if (TemasMap.containsKey(ev.getTopic())){
            TemasMap.get(ev.getTopic()).encola(ev);
            for (Subscriber Sub : subscriberListByTopic(ev.getTopic())) {

               SubscriberImpl S = (SubscriberImpl)Sub;
                S.EnviarEvento(ev);
                
            }
            return true;}
            else{return false;}
            
        } catch (Exception e) {
            System.err.println("error");
            throw new RemoteException("Error publicando evento", e);                    
           }
        
        
    }
    public synchronized Event consumeEvent(String topic) throws RemoteException {
          
             if (!TemasMap.containsKey(topic)){
                throw new NoSuchObjectException("sin tema");
             }
             else{


                try {
                    return  TemasMap.get(topic).decola();  } 
                catch (Exception e) 
                    {
                    System.err.println("error");
                    throw new RemoteException("Error publicando evento", e);                    
               
                }
            }
    }
    public synchronized Subscriber initSubscriber(SubscriberCallback c) throws RemoteException {
        Subscriber S = new SubscriberImpl(this,c) ;
        SubsList.add(S);
        return S;

        
    }
    public synchronized Collection<Subscriber> subscriberList() throws RemoteException {
        return SubsList;
    }
    public synchronized Collection<Subscriber> subscriberListByTopic(String topic) throws RemoteException {

        List <Subscriber> result = new ArrayList<>();

        if(TemasMap.containsKey(topic)){
            for (Subscriber S: SubsList) {
                if(S.topicListBySubscriber().contains(topic)){
                    result.add(S);
                }

                
            }
            return result;
        }
        else{
        return null;}
    }
    public synchronized boolean deleteTopic(String topic) throws RemoteException {
       return false;
    }
    static public void main (String args[])  {
        if (args.length!=1) {
            System.err.println("Usage: PubSubImpl registryPortNumber");
            return;
        }
        try {
            PubSub ps = new PubSubImpl();
            Server.init(ps, args[0]);
        }
        catch (Exception e) {
            System.err.println("PubSubImpl exception: " + e.toString());
            System.exit(1);
        }
    }
}
