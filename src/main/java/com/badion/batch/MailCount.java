package com.badion.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple4;

public class MailCount {

	public final static String MAIL_FIELD_DELIM = "#|#";
	public final static String MAIL_RECORD_DELIM = "##//##";


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setString("fs.overwrite-files", "true");
		String input = "/home/cloudera/pet-projects/mails.txt";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<String, String, String>> mails =
			env.readCsvFile(input)
				.lineDelimiter(MAIL_RECORD_DELIM)
				.fieldDelimiter(MAIL_FIELD_DELIM)
				.includeFields("0111")
				.ignoreInvalidLines()  // skip first field, use second and third and fourth
				.types(String.class, String.class, String.class);
		DataSet<Tuple4<String,String,String,Integer>> cleanData = 	mails
				.map(new MonthEmailExtractor())
				.groupBy(0, 1).reduceGroup(new MailCounter());


		DataSet<Tuple4<String,String,String,Integer>> apacheGit = cleanData
				.filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
					@Override
					public boolean filter(Tuple4<String, String, String, Integer> tuple4) throws Exception {
						return tuple4._3().equals("git@git.apache.org");
					}
				});
        apacheGit.print();
		apacheGit.writeAsText("/home/cloudera/pet-projects/git-emails.txt").withParameters(conf);
        DataSet<Tuple4<String,String,String,Integer>> apacheJira = cleanData
                .filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple4<String, String, String, Integer> tuple4) throws Exception {
                        return tuple4._3().equals("jira@apache.org");
                    }
                });
        apacheJira.print();
        apacheJira.writeAsText("/home/cloudera/pet-projects/jira-emails.txt").withParameters(conf);
	}

	public static class MonthEmailExtractor implements MapFunction<Tuple3<String,String, String>, Tuple3<String,String, String>> {

		@Override
		public Tuple3<String, String, String> map(Tuple3<String,String, String> mail) throws Exception {
			String month = mail.f0.substring(0, 7);
			String name = mail.f1.replaceAll("[^a-zA-Z\\d\\s:]","").toLowerCase().replaceAll("utf8","").substring(0, 10);
			String email = mail.f1.substring(mail.f1.lastIndexOf("<") + 1, mail.f1.length() - 1);
			return new Tuple3<>(month, name, email);
		}
	}

	public static class MailCounter implements GroupReduceFunction<Tuple3<String, String, String>, Tuple4<String ,String, String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple3<String,String, String>> mails, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {

			String month = null;
			String name = null;
			String email = null;
			int cnt = 0;

			for(Tuple3<String, String, String> m : mails) {
				month = m.f0;
				name = m.f1;
				email = m.f2;
				cnt++;
			}
			out.collect(new Tuple4<>(month, name, email, cnt));
		}
	}
}
