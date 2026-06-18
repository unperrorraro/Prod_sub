// clase Topic
package broker;
import java.util.concurrent.BlockingQueue;
import javax.lang.model.element.Element;
import pubsub.Event;
import pubsub.Subscriber;
import java.util.Queue;

import java.util.concurrent.ArrayBlockingQueue;
class Topic {
    private BlockingQueue <Event> Cola;
    public Topic() {
        Cola  = new ArrayBlockingQueue <Event>(100,true);
    }
    public void encola (Event ev) throws  Exception{
        try{
        Cola.put(ev); }
        catch(Exception e){

             System.err.println("error cola");
                    throw e;

        }
    }
     public Event decola () throws Exception{
       try {
        return  Cola.poll();
           
       } catch (Exception e) {
                    System.err.println("error cola");
                    throw e;


       }
        
    }
}
