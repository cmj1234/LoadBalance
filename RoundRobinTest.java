package com.jd.ihosp.channel;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 1、轮询策略
 * 2、随机策略
 * 3、权重算法
 * 4、平滑加权轮询算法
 * 5、一致性哈希核心-哈希环（基于 2^32 做取模，增加虚拟节点平衡请求）
 */
public class RoundRobinTest {

    // 用于记录当前请求的序列号
    static AtomicInteger requestIndex = new AtomicInteger(0);

    // 模拟配置的集群节点
    public static List<String> SERVERS = Arrays.asList(
            "192.168.0.1:8080",
            "192.168.0.1:8081",
            "192.168.0.1:8082",
            "192.168.0.1:8083",
            "192.168.0.1:8084"
    );


    // 在之前是Servers类中再加入一个权重服务列表
    public static Map<String, Integer> WEIGHT_SERVERS = new LinkedHashMap<>();
    static {
        // 配置集群的所有节点信息及权重值
        WEIGHT_SERVERS.put("192.168.0.1:8080",17);
        WEIGHT_SERVERS.put("192.168.0.1:8081",11);
        WEIGHT_SERVERS.put("192.168.0.1:8082",30);
    }

    public static void main(String[] args){
        // 使用for循环简单模拟10个客户端请求
//        for (int i = 1; i <= 10; i++){
//            System.out.println("轮询策略---第"+ i + "个请求：" + getRoundRobinServer());
//        }
//
//        for (int i = 1; i <= 10; i++){
//            System.out.println("随机策略---第"+ i + "个请求：" + getRandomServer());
//        }
//
//        for (int i = 1; i <= 10; i++){
//            System.out.println("随机权重策略---第"+ i + "个请求：" + getRandomWeightServer());
//        }

//        for (int i = 1; i <= 10; i++){
//            System.out.println("平滑加权轮询策略---第"+ i + "个请求：" + getServer());
//        }


        for (int i = 1; i <= 5; i++){
            System.out.println("第"+ i + "个请求：" + getServer("192.168.12.13"+i));
        }

    }

    /**
     *  1、轮询策略
     *  优点：1、算法实现简单，请求分发效率够高。
     *  2、能够将所有请求均摊到集群中的每个节点上。
     *  3、易于后期弹性伸缩，业务增长时可以拓展节点，业务萎靡时可以缩减节点。
     *
     *  缺点：1、对于不同配置的服务器无法合理照顾，无法将高配置的服务器性能发挥出来。
     *  2、由于请求分发时，是基于请求序列号来实现的，所以无法保证同一客户端的请求都是由同一节点处理的，因此需要通过 session 记录状态时，无法确保其一致性。
     *
     *  应用场景：1、集群中所有节点硬件配置都相同的情况。 2、只读不写，无需保持状态的情景。
     * @return String
     */
    public static String getRoundRobinServer(){
        // 用请求序列号取余集群节点数量，求得本次处理请求的节点下标
        int index = requestIndex.get() % SERVERS.size();
        // 从服务器列表中获取具体的节点IP地址信息
        String server = SERVERS.get(index);
        // 自增一次请求序列号，方便下个请求计算
        requestIndex.incrementAndGet();
        // 返回获取到的服务器IP地址
        return server;
    }

    /**
     * 2、随机策略
     * 劣势：1、无法合理的将请求均摊到每台服务器节点。
     * 2、由于处理请求的目标服务器不明确，因此也无法满足需要记录状态的请求。
     * 3、能够在一定程度上发挥出高配置的机器性能，但充满不确定因素。
     */
    public static String getRandomServer(){
        // 从已配置的服务器列表中，随机抽取一个节点处理请求
        return SERVERS.get(new java.util.Random().nextInt(SERVERS.size()));
    }


    /**
     * 3、随机权重算法
     * 1、先求和所有的权重值，再随机生成一个总权重之内的索引。
     *
     * 2、遍历之前配置的服务器列表，用随机索引与每个节点的权重值进行判断。如果小于，则代表当前请求应该落入目前这个节点；如果大于，则代表随机索引超出了目前节点的权重范围，则减去当前权重，继续与其他节点判断。
     *
     * 3、最终随机出的索引总会落入到一个节点的权重范围内，最后返回对应的节点 IP。
     */
    public static String getRandomWeightServer(){
        // 计算总权重值
        int weightTotal = 0;
        for (Integer weight : WEIGHT_SERVERS.values()) {
            weightTotal += weight;
        }
        // 从总权重的范围内随机生成一个索引
        int index = new java.util.Random().nextInt(weightTotal);
        // 遍历整个权重集群的节点列表，选择节点处理请求
        String targetServer = "";
        for (String server : WEIGHT_SERVERS.keySet()) {
            // 获取每个节点的权重值
            Integer weight = WEIGHT_SERVERS.get(server);
            // 如果权重值大于产生的随机数，则代表此次随机分配应该落入该节点
            if (weight > index){
                // 直接返回对应的节点去处理本次请求并终止循环
                targetServer = server;
                break;
            }
            // 如果当前节点的权重值小于随机索引，则用随机索引减去当前节点的权重值，
            // 继续循环权重列表，与其他的权重值进行对比，
            // 最终该请求总会落入到某个IP的权重值范围内
            index = index - weight;
        }
        // 返回选中的目标节点
        return targetServer;
    }

    /**
     * 4、平滑加权轮询算法
     */

    // 初始化存储每个节点的权重容器
    private static Map<String,Weight> weightMap = new HashMap<>();

    // 计算总权重值，只需要计算一次，因此放在静态代码块中执行
    private static int weightTotal = 0;
    static {
        sumWeightTotal();
    }

    // 求和总权重值，后续动态伸缩节点时，再次调用该方法即可。
    public static void sumWeightTotal(){
        for (Integer weight : WEIGHT_SERVERS.values()) {
            weightTotal += weight;
        }
    }

    // 获取处理本次请求的具体服务器IP
    public static String getServer(){
        // 判断权重容器中是否有节点信息
        if (weightMap.isEmpty()){
            // 如果没有则将配置的权重服务器列表挨个载入容器
            WEIGHT_SERVERS.forEach((servers, weight) -> {
                // 初始化时，每个节点的动态权重值都为0
                weightMap.put(servers, new Weight(servers, weight, 0));
            });
        }

        // 每次请求时，更改动态权重值
        for (Weight weight : weightMap.values()) {
            weight.setCurrentWeight(weight.getCurrentWeight()
                    + weight.getWeight());
        }

        // 判断权重容器中最大的动态权重值
        Weight maxCurrentWeight = null;
        for (Weight weight : weightMap.values()) {
            if (maxCurrentWeight == null || weight.getCurrentWeight()
                    > maxCurrentWeight.getCurrentWeight()){
                maxCurrentWeight = weight;
            }
        }

        // 最后用最大的动态权重值减去所有节点的总权重值
        maxCurrentWeight.setCurrentWeight(maxCurrentWeight.getCurrentWeight()
                - weightTotal);

        // 返回最大的动态权重值对应的节点IP
        return maxCurrentWeight.getServer();
    }

    /**
     * 5、一致性哈希核心-哈希环
     */
    // 使用有序的红黑树结构，用于实现哈希环结构
    private static final TreeMap<Integer,String> virtualNodes = new TreeMap<>();
    // 每个真实节点的虚拟节点数量
    private static final int VIRTUAL_NODES = 160;

    static {
        // 对每个真实节点添加虚拟节点，虚拟节点会根据哈希算法进行散列
        for (String serverIP : SERVERS) {
            // 将真实节点的IP映射到哈希环上
            virtualNodes.put(getHashCode(serverIP), serverIP);
            // 根据设定的虚拟节点数量进行虚拟节点映射
            for (int i = 0; i < VIRTUAL_NODES; i++){
                // 计算出一个虚拟节点的哈希值（只要不同即可）
                int hash = getHashCode(serverIP + i);
                // 将虚拟节点添加到哈希环结构上
                virtualNodes.put(hash, serverIP);
            }
        }
    }

    public static String getServer(String IP){
        int hashCode = getHashCode(IP);
        // 得到大于该Hash值的子红黑树
        SortedMap<Integer, String> sortedMap = virtualNodes.tailMap(hashCode);
        // 得到该树的第一个元素，也就是最小的元素
        Integer treeNodeKey = sortedMap.firstKey();
        // 如果没有大于该元素的子树了，则取整棵树的第一个元素，相当于取哈希环中的最小元素
        if (sortedMap == null)
            treeNodeKey = virtualNodes.firstKey();
        // 返回对应的虚拟节点名称
        return virtualNodes.get(treeNodeKey);
    }

    // 哈希方法：用于计算一个IP的哈希值
    public static int getHashCode(String IP){
        final int p = 1904390101;
        int hash = (int)1901102097L;
        for (int i = 0; i < IP.length(); i++)
            hash = (hash ^ IP.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        // 如果算出来的值为负数则取其绝对值
        if (hash < 0)
            hash = Math.abs(hash);
        return hash;
    }



    // 权重类
    public static class Weight {
        // 节点信息
        private String server;
        // 节点权重值
        private Integer weight;
        // 动态权重值
        private Integer currentWeight;

        // 构造方法
        public Weight() {}
        public Weight(String server, Integer weight, Integer currentWeight) {
            this.server = server;
            this.weight = weight;
            this.currentWeight = currentWeight;
        }

        // 封装方法
        public String getServer() {
            return server;
        }
        public void setServer(String server) {
            this.server = server;
        }
        public Integer getWeight() {
            return weight;
        }
        public void setWeight(Integer weight) {
            this.weight = weight;
        }
        public Integer getCurrentWeight() {
            return this.currentWeight;
        }
        public void setCurrentWeight(Integer currentWeight) {
            this.currentWeight = currentWeight;
        }
    }

}
