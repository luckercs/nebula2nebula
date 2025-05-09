import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.*;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphNebulaPool {
    private static final Logger LOG = LoggerFactory.getLogger(GraphNebulaPool.class);

    private NebulaPool pool = new NebulaPool();
    private NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
    private String graphdAddressHost;
    private int graphdAddressPort;
    private List<HostAddress> addresses;
    private String user;
    private String pass;

    public GraphNebulaPool(String graphdAddress, String user, String pass) throws UnknownHostException {
        this.graphdAddressHost = graphdAddress.split(":")[0];
        this.graphdAddressPort = Integer.parseInt(graphdAddress.split(":")[1]);
        this.addresses = Arrays.asList(new HostAddress(graphdAddressHost, graphdAddressPort));
        this.user = user;
        this.pass = pass;
        nebulaPoolConfig.setMaxConnSize(100);
        nebulaPoolConfig.setTimeout(10000);
        LOG.info("Initialize Nebula Graph session pool with address: {}, user: {}", graphdAddress, user);
        pool.init(addresses, nebulaPoolConfig);
    }

    public Session getSession() throws IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException {
        Session session;
        session = pool.getSession(user, pass, false);
        return session;
    }

    public void close() {
        try {
            pool.close();
        } catch (Exception e) {
        }
    }
}
