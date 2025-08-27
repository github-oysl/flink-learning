package com.zhisheng.examples.batch.wordcount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * batch
 */
/**
 * Flink 批处理 WordCount 示例。
 * <p>
 * 这个类演示了如何使用 Flink 的 DataSet API 来实现一个经典的单词计数程序。
 * 它从一个固定的字符串数组中读取文本数据，然后进行转换、分组、聚合，
 * 最终将每个单词的出现次数打印到控制台。
 * <p>
 * 这是学习 Flink 批处理的基础入门示例。
 *
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {

    /**
     * Flink 作业的入口方法。
     *
     * @param args 命令行参数，可以通过 ParameterTool 进行解析。
     * @throws Exception 可能在作业执行期间抛出异常。
     */
    public static void main(String[] args) throws Exception {
        // 1. 从 main 方法的 args 参数中解析配置，例如 --input, --output
        final ParameterTool params = ParameterTool.fromArgs(args);
        // 2. 获取 Flink 的批处理执行环境 (DataSet API)
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 3. 将解析的参数设置为作业的全局配置，这样在所有函数中都可以访问
        env.getConfig().setGlobalJobParameters(params);

        // 4. 使用 fromElements 从一个内存中的字符串数组创建数据源 (DataSource)
        env.fromElements(WORDS)
                // 5. 使用 flatMap 算子进行转换：将每一行文本拆分成单词，并转换为 (单词, 1) 的形式
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 5.1. 将输入的字符串（行）转换为小写，并使用非单词字符(\W+)进行分割
                        String[] splits = value.toLowerCase().split("\\W+");

                        // 5.2. 遍历所有拆分出的单词
                        for (String split : splits) {
                            // 5.3. 确保单词不是空的
                            if (split.length() > 0) {
                                // 5.4. 将每个单词构造成一个 (word, 1) 的二元组，并发送到下游
                                // 这里的 1 代表这个单词出现了一次
                                out.collect(new Tuple2<>(split, 1));
                            }
                        }
                    }
                })
                // 6. 使用 groupBy(0) 按元组的第一个字段（即单词）进行分组
                .groupBy(0)
                // 7. 使用 reduce 算子对每个分组内的数据进行聚合
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        // 7.1. reduce 函数接收两个输入，它们来自同一个分组（即单词相同）
                        // 7.2. 返回一个新的元组，单词保持不变(value1.f0)，计数器是两个输入值的和(value1.f1 + value2.f1)
                        //      例如，对于 ("hello", 1) 和 ("hello", 1)，reduce 后会得到 ("hello", 2)
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                // 8. 使用 print() 算子将最终结果打印到控制台。
                //    print() 是一个 "Action" (行动) 算子，它会触发整个 Flink 作业的执行。
                .print();
    }

    // 预设的输入数据
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
