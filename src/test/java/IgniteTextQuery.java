import model.Customer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IgniteTextQuery {
    private final IgniteConfiguration igniteConfiguration;
    private Ignite client;

    public IgniteTextQuery() {
        igniteConfiguration = new IgniteConfiguration();
//cfg.setPeerClassLoadingEnabled(true);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSlowClientQueueLimit(1000);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipfinder = new TcpDiscoveryVmIpFinder();
        ipfinder.setAddresses(Collections.singletonList("localhost"));
        discoverySpi.setIpFinder(ipfinder);

        igniteConfiguration.setCommunicationSpi(commSpi);
        igniteConfiguration.setDiscoverySpi(discoverySpi);
    }

    @BeforeTest
    public void initCache() throws Exception {
        Ignition.setClientMode(true);
        client = Ignition.start(igniteConfiguration);
        if (client.cacheNames().contains("customer"))
            client.destroyCache("customer");
        if (client.cacheNames().contains("customerPerf"))
            client.destroyCache("customerPerf");

        CacheConfiguration<Long, Customer> ccfg = new CacheConfiguration<>();
        ccfg.setName("customer");
        // Registering indexed type.
        ccfg.setIndexedTypes(Long.class, Customer.class);

        IgniteCache<Long, Customer> custCache = client.getOrCreateCache(ccfg);
        custCache.put(1L, new Customer(1L, "Nicolas", "Tesla"));
        custCache.put(2L, new Customer(2L, "Nicolas", "White"));
        custCache.put(3L, new Customer(3L, "Thomas", "Edison"));
        custCache.put(4L, new Customer(4L, "Nick", "Tesla"));
        custCache.put(5L, new Customer(5L, "Nichol", "Tesla"));
        custCache.put(6L, new Customer(6L, "Nikola", "Tesla")); //Correct

        CacheConfiguration<Long, Customer> confPerf = new CacheConfiguration<>();
        confPerf.setName("customerPerf");
        // Registering indexed type.
        confPerf.setIndexedTypes(Long.class, Customer.class);

        IgniteCache<Long, Customer> cache = client.getOrCreateCache(confPerf);

        try (IgniteDataStreamer<Long, Customer> stmr = client.dataStreamer("customerPerf")) {
            // Configure loader.
            stmr.perNodeBufferSize(1024);
            stmr.perNodeParallelOperations(8);

            for (int i = 0; i < 1000000; i++) {
                stmr.addData((long) i, new Customer((long) i, "Gokhan" + i, "Imral" + i % 100));
                // Print out progress while loading cache.
                if (i > 0 && i % 10000 == 0)
                    System.out.println("Loaded " + i + " keys.");
            }
        }
        //cache.put((long) i, new Customer((long) i,"Gokhan" + i,"Imral" + i % 100));


    }

//    @Test
//    public void testCacheGet() throws Exception {
//        try (IgniteClient client = Ignition.startClient(cfg)) {
//            ClientCache<Long, Customer> cache = client.cache("customer");
//            // Get data from the cache
//            Customer c = cache.get(1L);
//            Assert.assertEquals(c.getName(),"Gokhans");
//        }
//    }

    @Test
    public void testFuzzySearchMaxStep() throws Exception {

        IgniteCache<Long, Customer> cache = client.cache("customer");
        TextQuery<Long, Customer> qry =
                new TextQuery<>(Customer.class, "name:Nicoles~2 AND surname:Tesla");
        List<Cache.Entry<Long, Customer>> customers = cache.query(qry).getAll();
        List<Long> customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
        ArrayList<Long> expected = new ArrayList<>();
        System.out.println(customers);
        expected.add(1L);
        Assert.assertEquals(customerIds, expected);

        qry =
                new TextQuery<>(Customer.class, "name:Nikolas~2 AND surname:Tesla");
        customers = cache.query(qry).getAll();
        customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
        expected = new ArrayList<>();
        System.out.println(customers);
        expected.add(1L);
        expected.add(6L);
        Assert.assertEquals(customerIds, expected);

        qry =
                new TextQuery<>(Customer.class, "name:Nikoles~2 AND surname:Tesla");
        customers = cache.query(qry).getAll();
        customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
        expected = new ArrayList<>();
        System.out.println(customers);
        expected.add(1L);
        expected.add(6L);
        Assert.assertEquals(customerIds, expected);
    }

//    @Test
//    public void testMultiWordFuzzySearchMaxStep() throws Exception {
//
//        IgniteCache<Long, Customer> cache = client.cache("customer");
//        TextQuery<Long, Customer> qry =
//                new TextQuery<>(Customer.class, "name:Nicoles~2 AND surname:Tesla");
//        List<Cache.Entry<Long, Customer>> customers = cache.query(qry).getAll();
//        List<Long> customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
//        ArrayList<Long> expected = new ArrayList<>();
//        System.out.println(customers);
//        expected.add(1L);
//        Assert.assertEquals(customerIds, expected);
//
//        qry =
//                new TextQuery<>(Customer.class, "name:Nikolas~2 AND surname:Tesla");
//        customers = cache.query(qry).getAll();
//        customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
//        expected = new ArrayList<>();
//        System.out.println(customers);
//        expected.add(1L);
//        expected.add(6L);
//        Assert.assertEquals(customerIds, expected);
//
//        qry =
//                new TextQuery<>(Customer.class, "name:Nikoles~2 AND surname:Tesla");
//        customers = cache.query(qry).getAll();
//        customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
//        expected = new ArrayList<>();
//        System.out.println(customers);
//        expected.add(1L);
//        expected.add(6L);
//        Assert.assertEquals(customerIds, expected);
//    }

    @Test
    public void testFuzzySearchOneStep() throws Exception {

        IgniteCache<Long, Customer> cache = client.cache("customer");
        // Get data from the cache
        TextQuery<Long, Customer> qry =
                new TextQuery<>(Customer.class, "name:Nicoles~1 AND surname:Tesla");
        List<Cache.Entry<Long, Customer>> customers = cache.query(qry).getAll();
        List<Long> customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
        ArrayList<Long> expected = new ArrayList<>();
        System.out.println(customers);
        expected.add(1L);
        Assert.assertEquals(customerIds, expected);

        qry =
                new TextQuery<>(Customer.class, "name:Nikolas~1 AND surname:Tesla");
        customers = cache.query(qry).getAll();
        customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
        expected = new ArrayList<>();
        System.out.println(customers);
        expected.add(1L);
        expected.add(6L);
        Assert.assertEquals(customerIds, expected);

        qry =
                new TextQuery<>(Customer.class, "name:Nikoles~1 AND surname:Tesla");
        customers = cache.query(qry).getAll();
        customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
        expected = new ArrayList<>();
        System.out.println(customers);
        Assert.assertEquals(customerIds, expected);
    }

    @Test
    public void testFuzzySearchHalfStep() throws Exception {

        IgniteCache<Long, Customer> cache = client.cache("customer");
        // Get data from the cache
        TextQuery<Long, Customer> qry =
                new TextQuery<>(Customer.class, "name:Nicolas~0.5 AND surname:Tesla");
        List<Cache.Entry<Long, Customer>> customers = cache.query(qry).getAll();
        List<Long> customerIds = customers.stream().map(c -> c.getValue().getCustomerId()).sorted().collect(Collectors.toList());
        ArrayList<Long> expected = new ArrayList<>();
        System.out.println(customers);
        expected.add(1L);
        expected.add(6L);
        Assert.assertEquals(customerIds, expected);
    }

    @Test
    public void testFuzzySearchPerformance() throws Exception {
        IgniteCache<Long, Customer> cache = client.cache("customerPerf");
        long elapsedTime = 0L;
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            TextQuery<Long, Customer> qry =
                    new TextQuery<>(Customer.class, "name:Gokhan" + i * 1000 + "~2 AND surname:Imral2");
            List<Cache.Entry<Long, Customer>> customers = cache.query(qry).getAll();

            long end = System.currentTimeMillis();
            elapsedTime += end - start;
            Assert.assertTrue(customers.size() > 0);
        }
        System.out.println(elapsedTime / 100);

    }

    @Test
    public void testCacheGetPerformance() throws Exception {
        IgniteCache<Long, Customer> cache = client.cache("customerPerf");
        long start = System.currentTimeMillis();

        Customer customer = cache.get(100L);

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        Assert.assertNotNull(customer);
        System.out.println(elapsedTime);
    }

    @AfterTest
    public void destroyCache() throws Exception {

        client.destroyCache("customer");
        client.destroyCache("customerPerf");
        client.close();
    }
}
