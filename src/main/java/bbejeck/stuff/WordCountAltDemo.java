package bbejeck.stuff;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;

public class WordCountAltDemo {


    public static void main(String[] args) {
        // Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // Construct a `KStream` from the input topic ""streams-file-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "streams-file-input");

        KStream<String, Long> wordCounts = textLines

                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))

                // We will subsequently invoke `countByKey` to count the occurrences of words, so we use
                // `map` to ensure the words are available as message keys, too.
                .map((key, value) -> new KeyValue<>(value, value))



                // Count the occurrences of each word (message key).
                // This will change the stream type from `KStream<String, String>` to
                // `KTable<String, Long>` (word -> count), hence we must provide serdes for `String`
                // and `Long`.
                //
                .countByKey(stringSerde, "Counts")
                // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
                .toStream();

        // Write the `KStream<String, Long>` to the output topic.
        wordCounts.to(stringSerde, longSerde, "streams-wordcount-output");
    }
}
