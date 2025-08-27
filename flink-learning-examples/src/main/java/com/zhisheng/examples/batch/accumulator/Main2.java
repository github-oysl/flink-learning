package com.zhisheng.examples.batch.accumulator;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * Desc: 累加器
 * Created by zhisheng on 2019-06-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main2 {

    public static void main(String[] args) throws Exception {
        // 1. 从 main 方法的 args 参数中解析配置，例如 --input, --output
        final ParameterTool params = ParameterTool.fromArgs(args);
        // 2. 获取 Flink 的批处理执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 3. 将解析的参数设置为作业的全局配置，这样在所有函数中都可以访问
        env.getConfig().setGlobalJobParameters(params);

        // 4. 从一个固定的 String 数组创建数据源
        DataSource<String> dataSource = env.fromElements(WORDS);

        // 5. 对数据源进行 flatMap 操作，处理每一行数据
        dataSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

            // 6. 创建一个整型累加器实例，用于统计行数
            private IntCounter linesNum = new IntCounter();

            /**
             * open 方法是 RichFunction 的生命周期方法之一。
             * 它在算子（这里是 flatMap）的实例初始化时调用一次，在处理任何数据之前。
             * 这里我们用它来注册累加器。
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                // 7. 在 Flink 运行时上下文中注册累加器，并命名为 "linesNum"
                getRuntimeContext().addAccumulator("linesNum", linesNum);
            }

            /**
             * flatMap 方法是核心处理逻辑，对输入的每一行（line）进行操作。
             */
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 8. 使用非单词字符（如空格、逗号、冒号等）作为分隔符，将行拆分为单词
                String[] words = line.split("\\W+");
                // 9. 遍历拆分出的所有单词
                for (String word : words) {
                    // 10. 将每个单词构造成一个 (word, 1) 的二元组，并发送到下游
                    out.collect(new Tuple2<>(word, 1));
                }

                // 11. 每处理完一行数据，就让累加器加 1
                linesNum.add(1);
            }
        })
                // 12. 按照单词（元组的第一个元素，索引为 0）进行分组
                .groupBy(0)
                // 13. 对每个分组内的数据进行聚合，将计数（元组的第二个元素，索引为 1）求和
                .sum(1)
                // 14. 打印结果到控制台。这个 print() 方法是一个 Action，它会触发整个 Flink 作业的执行。
                .print();

        // 15. 作业执行后，通过 getLastJobExecutionResult() 获取作业执行结果对象
        //     然后调用 getAccumulatorResult("linesNum") 方法，根据注册的名称获取累加器的最终值
        int linesNum = env.getLastJobExecutionResult().getAccumulatorResult("linesNum");
        // 16. 打印累加器统计的总行数
        System.out.println("Total lines: " + linesNum);
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
