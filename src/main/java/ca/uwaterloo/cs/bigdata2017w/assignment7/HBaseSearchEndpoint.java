/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HBaseSearchEndpoint extends Configured implements Tool {

  private HBaseSearchEndpoint() {}

  private static final class Args {
    @Option(name = "-config", metaVar = "[path]", required = true, usage = "config")
    String config;

    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index table")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection table")
    String collection;

    @Option(name = "-port", metaVar = "[port]", required = true, usage = "port")
    int port;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    System.out.println("Port: " + args.port);
    System.out.println("Index: " + args.index);
    System.out.println("Collection: " + args.collection);
    System.out.println("Config: " + args.config);

    System.out.println("Starting Server");

    Server server = new Server(args.port);
    ServletContextHandler handler = new ServletContextHandler(server, "/");

    JettyServer jetty = new JettyServer(args.index, args.collection, args.config);
    ServletHolder jettyHolder = new ServletHolder(jetty);
    handler.addServlet(jettyHolder, "/search");

    server.start();

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new HBaseSearchEndpoint(), args);
  }
}
