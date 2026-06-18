
// clase estática que contacta con el Registry para dar de alta el servicio
package broker;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import pubsub.PubSub;

// no se puede instanciar ni derivar
public final class Server {
    private Server(){};
    public String echo(String s) throws RemoteException {
        return s.toUpperCase();
    }
    static void init(PubSub pubsub, String port) throws RemoteException {
        try {

            Registry registry = LocateRegistry.getRegistry(Integer.parseInt(port));
            registry.rebind("1", pubsub);

            
        } catch (Exception e) {
             System.err.println("EchoServer exception:");
            e.printStackTrace(); System.exit(1);
            
        }
    }
}