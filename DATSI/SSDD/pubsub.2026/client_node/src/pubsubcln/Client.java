// clase estática que contacta con el Registry para obtener la
// referencia remota al servicio

// DEBE RELLENAR EL MÉTODO init

package pubsubcln;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import pubsub.PubSub;

// no se puede instanciar ni derivar
public final class Client {
    private Client(){};
    static public PubSub init(String host, String port) throws RemoteException, NotBoundException {
        try {
               Registry registry = LocateRegistry.getRegistry(host,
			    Integer.parseInt(port));
            // obtiene una referencia remota el servicio
            PubSub srv = (PubSub) registry.lookup("1");


            return srv;
        } catch (Exception e) {
         System.err.println("EchoClient exception:"); e.printStackTrace();
         throw e;
         

        }
        
    }
}
