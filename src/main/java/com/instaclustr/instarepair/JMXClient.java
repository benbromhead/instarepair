package com.instaclustr.instarepair;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.*;

public class JMXClient {
    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";
    private static final String ssObjName = "org.apache.cassandra.db:type=StorageService";
    private static final int defaultPort = 7199;
    final String host;
    final int port;
    private String username;
    private String password;

    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;
    private CompactionManagerMBean compactionProxy;

    private boolean failed;

    public JMXClient(String host, int port) throws IOException
    {
        this.host = host;
        this.port = port;
        connect();
    }

    /**
     * Creates a NodeProbe using the specified JMX host and default port.
     *
     * @param host hostname or IP address of the JMX agent
     * @throws IOException on connection failures
     */
    public JMXClient(String host) throws IOException
    {
        this.host = host;
        this.port = defaultPort;
        connect();
    }

    public Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> getColumnFamilyStoreMBeanProxies()
    {
        try
        {
            return new ColumnFamilyStoreMBeanIterator(mbeanServerConn);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not retrieve list of stat mbeans.", e);
        }
    }

    public CompactionManagerMBean getCompactionManagerProxy()
    {
        return compactionProxy;
    }

    private void connect() throws IOException
    {
        JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
        Map<String,Object> env = new HashMap<String,Object>();
        if (username != null)
        {
            String[] creds = { username, password };
            env.put(JMXConnector.CREDENTIALS, creds);
        }

        env.put("com.sun.jndi.rmi.factory.socket", getRMIClientSocketFactory());

        jmxc = JMXConnectorFactory.connect(jmxUrl, env);
        mbeanServerConn = jmxc.getMBeanServerConnection();

        try
        {
            ObjectName name = new ObjectName(CompactionManager.MBEAN_OBJECT_NAME);
            compactionProxy = JMX.newMBeanProxy(mbeanServerConn, name, CompactionManagerMBean.class);

        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(
                    "Invalid ObjectName? Please report this as a bug.", e);
        }
    }

    private RMIClientSocketFactory getRMIClientSocketFactory()
    {
        if (Boolean.parseBoolean(System.getProperty("ssl.enable")))
            return new SslRMIClientSocketFactory();
        else
            return RMISocketFactory.getDefaultSocketFactory();
    }

    class ColumnFamilyStoreMBeanIterator implements Iterator<Map.Entry<String, ColumnFamilyStoreMBean>>
    {
        private MBeanServerConnection mbeanServerConn;
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> mbeans;

        public ColumnFamilyStoreMBeanIterator(MBeanServerConnection mbeanServerConn)
                throws MalformedObjectNameException, NullPointerException, IOException
        {
            this.mbeanServerConn = mbeanServerConn;
            List<Map.Entry<String, ColumnFamilyStoreMBean>> cfMbeans = getCFSMBeans(mbeanServerConn, "ColumnFamilies");
            cfMbeans.addAll(getCFSMBeans(mbeanServerConn, "IndexColumnFamilies"));
            Collections.sort(cfMbeans, new Comparator<Map.Entry<String, ColumnFamilyStoreMBean>>()
            {
                public int compare(Map.Entry<String, ColumnFamilyStoreMBean> e1, Map.Entry<String, ColumnFamilyStoreMBean> e2)
                {
                    //compare keyspace, then CF name, then normal vs. index
                    int keyspaceNameCmp = e1.getKey().compareTo(e2.getKey());
                    if(keyspaceNameCmp != 0)
                        return keyspaceNameCmp;

                    // get CF name and split it for index name
                    String e1CF[] = e1.getValue().getColumnFamilyName().split("\\.");
                    String e2CF[] = e2.getValue().getColumnFamilyName().split("\\.");
                    assert e1CF.length <= 2 && e2CF.length <= 2 : "unexpected split count for table name";

                    //if neither are indexes, just compare CF names
                    if(e1CF.length == 1 && e2CF.length == 1)
                        return e1CF[0].compareTo(e2CF[0]);

                    //check if it's the same CF
                    int cfNameCmp = e1CF[0].compareTo(e2CF[0]);
                    if(cfNameCmp != 0)
                        return cfNameCmp;

                    // if both are indexes (for the same CF), compare them
                    if(e1CF.length == 2 && e2CF.length == 2)
                        return e1CF[1].compareTo(e2CF[1]);

                    //if length of e1CF is 1, it's not an index, so sort it higher
                    return e1CF.length == 1 ? 1 : -1;
                }
            });
            mbeans = cfMbeans.iterator();
        }

        private List<Map.Entry<String, ColumnFamilyStoreMBean>> getCFSMBeans(MBeanServerConnection mbeanServerConn, String type)
                throws MalformedObjectNameException, IOException
        {
            ObjectName query = new ObjectName("org.apache.cassandra.db:type=" + type +",*");
            Set<ObjectName> cfObjects = mbeanServerConn.queryNames(query, null);
            List<Map.Entry<String, ColumnFamilyStoreMBean>> mbeans = new ArrayList<Map.Entry<String, ColumnFamilyStoreMBean>>(cfObjects.size());
            for(ObjectName n : cfObjects)
            {
                String keyspaceName = n.getKeyProperty("keyspace");
                ColumnFamilyStoreMBean cfsProxy = JMX.newMBeanProxy(mbeanServerConn, n, ColumnFamilyStoreMBean.class);
                mbeans.add(new AbstractMap.SimpleImmutableEntry<String, ColumnFamilyStoreMBean>(keyspaceName, cfsProxy));
            }
            return mbeans;
        }

        public boolean hasNext()
        {
            return mbeans.hasNext();
        }

        public Map.Entry<String, ColumnFamilyStoreMBean> next()
        {
            return mbeans.next();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
