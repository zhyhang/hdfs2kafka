/**
 * 
 */
package com.yanhuang.bigdata.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * write specify files to kafka
 * 
 * @author zhyhang
 *
 */
public class File2Kafka {

	private static final String LOG_PREFIX_WHOLE_FILE_ERROR = "whole file to kafka error";

	private Path exceptionFileRoot = Paths.get("/tmp");

	private String kafkaTopic = "test";

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private ConcurrentMap<Path, BufferedWriter> fileWriterMap = new ConcurrentHashMap<>();

	private Producer<String, String> kafkaProducer;

	{
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProducer = new KafkaProducer<>(props);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Path exceptionFileRoot = Paths.get("/tmp");
		Path file = Paths.get("/data/logs/part-00000");
		System.out.println(exceptionFileRoot.resolve("except." + file.toString().replace(File.separatorChar, '.')));

		File2Kafka toK = new File2Kafka();

		toK.close();
	}

	public void write(String dir, int lookupDepth, Predicate<String> fileLineFilter) {
		Path[] files = listFiles(Paths.get(dir), lookupDepth);
		// serialize deal with one file to another
		// you can parallel deal with files
		Stream.of(files).forEach(file -> {
			writeFile(file, fileLineFilter == null ? l -> true : fileLineFilter);
		});
	}

	private Path[] listFiles(Path dirPath, int lookupDepth) {
		try {
			return Files.walk(dirPath, lookupDepth).filter(((Predicate<Path>) Files::isDirectory).negate())
					.toArray(Path[]::new);
		} catch (Exception e) {
			logger.error("read files from the specify directory {} error", dirPath, e);
			return new Path[0];
		}
	}

	private void writeFile(Path file, Predicate<String> fileLineFilter) {
		try {
			// parallel write file lines，need memory for file data
			Files.readAllLines(file).stream().unordered().parallel().filter(fileLineFilter).forEach(line -> {
				writeLine(file, line);
			});
		} catch (Exception e) {
			logger.error(LOG_PREFIX_WHOLE_FILE_ERROR + " {}", file);
		}
	}

	private void writeLine(Path file, String line) {
		try {
			int index001 = line.indexOf('\001');
			int index002 = line.indexOf('\002');
			String uuid = null;
			if (index001 >= 0 && index002 >= 0) {
				uuid = line.substring(index001 + 1, index002);
			}
			if (uuid != null) {
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, uuid, line);
				kafkaProducer.send(record);
			} else {
				writeExceptionLine(file, line);
			}

		} catch (Exception e) {
			writeExceptionLine(file, line);
		}

	}

	private void writeExceptionLine(Path file, String line) {
		BufferedWriter bw = fileWriterMap.computeIfAbsent(file, f -> {
			try {
				Path exceptFile = exceptionFileRoot
						.resolve("except" + file.toString().replace(File.separatorChar, '.'));
				logger.info("exception file {} for data file {}", exceptFile, file);
				return Files.newBufferedWriter(exceptFile);
			} catch (Exception e) {
				logger.error("create filewriter {} error", file, e);
				logger.info("file exception line {} :\t {}", file, line);
				return null;
			}
		});
		try {
			bw.write(line);
			bw.write('\n');
		} catch (Exception e) {
			logger.info("file exception line {} :\t {}", file, line);
		}
	}

	public void close() {
		fileWriterMap.forEach((k, v) -> {
			try {
				v.close();
			} catch (Exception e) {
				logger.error("close filewriter {} error", k, e);
			}
		});
		kafkaProducer.close();
	}

}
