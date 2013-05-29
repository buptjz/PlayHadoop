/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for counting the number of pages in a particular Wikipedia XML dump file. This program keeps
 * track of total number of pages, redirect pages, disambiguation pages, empty pages, actual
 * articles (including stubs), stubs, and non-articles ("File:", "Category:", "Wikipedia:", etc.).
 * This also provides a skeleton for MapReduce programs to process the collection. Specify input
 * path to the Wikipedia XML dump file with the {@code -input} flag.
 *
 * @author Jimmy Lin
 * @author Peter Exner
 */
public class CountWikipediaPages extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(CountWikipediaPages.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, NON_ARTICLE
  };

  private static class MyMapper extends MapReduceBase implements
      Mapper<LongWritable, WikipediaPage, Text, IntWritable> {

    public void map(LongWritable key, WikipediaPage p, OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      reporter.incrCounter(PageTypes.TOTAL, 1);

      if (p.isRedirect()) {
        reporter.incrCounter(PageTypes.REDIRECT, 1);

      } else if (p.isDisambiguation()) {
        reporter.incrCounter(PageTypes.DISAMBIGUATION, 1);
      } else if (p.isEmpty()) {
        reporter.incrCounter(PageTypes.EMPTY, 1);
      } else if (p.isArticle()) {
        reporter.incrCounter(PageTypes.ARTICLE, 1);

        if (p.isStub()) {
          reporter.incrCounter(PageTypes.STUB, 1);
        }
      } else {
        reporter.incrCounter(PageTypes.NON_ARTICLE, 1);
      }
    }
  }

  private static final String INPUT_OPTION = "input";
  private static final String LANGUAGE_OPTION = "wiki_language";
  
  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));
    
    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    
    String language = null;
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if(language.length()!=2){
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - XML dump file: " + inputPath);
    LOG.info(" - language: " + language);
    
    JobConf conf = new JobConf(getConf(), CountWikipediaPages.class);
    conf.setJobName(String.format("CountWikipediaPages[%s: %s, %s: %s]", INPUT_OPTION, inputPath, LANGUAGE_OPTION, language));

    conf.setNumMapTasks(10);
    conf.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(conf, new Path(inputPath));

    if(language != null){
      conf.set("wiki.language", language);
    }
    
    conf.setInputFormat(WikipediaPageInputFormat.class);
    conf.setOutputFormat(NullOutputFormat.class);

    conf.setMapperClass(MyMapper.class);

    JobClient.runJob(conf);

    return 0;
  }

  public CountWikipediaPages() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new CountWikipediaPages(), args);
  }
}
