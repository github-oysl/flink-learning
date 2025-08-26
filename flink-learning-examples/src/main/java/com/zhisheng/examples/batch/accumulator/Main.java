package com.zhisheng.examples.batch.accumulator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * Desc: 累加器
 * Created by zhisheng on 2019-06-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // 从程序参数中创建 ParameterTool 对象，用于获取配置参数
        final ParameterTool params = ParameterTool.fromArgs(args);
        // 获取 Flink 的执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 设置全局作业参数
        env.getConfig().setGlobalJobParameters(params);

        // 从预定义的 WORDS 数组创建数据源
        DataSource<String> dataSource = env.fromElements(WORDS);

        // 对数据源进行词频统计处理
        dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 使用非单词字符分割输入行
                String[] words = line.split("\\W+");
                // 遍历每个单词，输出(单词,1)的二元组
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        })
                // 按照单词（元组的第一个元素）分组
                .groupBy(0)
                // 对相同单词的计数（元组的第二个元素）求和
                .sum(1)
                // 按照计数进行全局降序排序
                .sortPartition(1, org.apache.flink.api.common.operators.Order.DESCENDING)
                .setParallelism(1)
                // 打印结果
                .print();

        // 计算数据源中的总行数
        long count = dataSource.count();
        // 打印总行数
        System.out.println(count);
    }

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,",
            "And by opposing end them?--To die,--to sleep,--",
            "No more; and by a sleep to say we end",
            "The heartache, and the thousand natural shocks",
            "That flesh is heir to,--'tis a consummation",
            "Devoutly to be wish'd. To die,--to sleep;--",
            "To sleep! perchance to dream:--ay, there's the rub;",
            "For in that sleep of death what dreams may come,",
            "When we have shuffled off this mortal coil,",
            "Must give us pause: there's the respect",
            "That makes calamity of so long life;",
            "For who would bear the whips and scorns of time,",
            "The oppressor's wrong, the proud man's contumely,",
            "The pangs of despis'd love, the law's delay,",
            "The insolence of office, and the spurns",
            "That patient merit of the unworthy takes,",
            "When he himself might his quietus make",
            "With a bare bodkin? who would these fardels bear,",
            "To grunt and sweat under a weary life,",
            "But that the dread of something after death,--",
            "The undiscover'd country, from whose bourn",
            "No traveller returns,--puzzles the will,",
            "And makes us rather bear those ills we have",
            "Than fly to others that we know not of?",
            "Thus conscience does make cowards of us all;",
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;",
            "And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
    };
}
