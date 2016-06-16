package org.tallison.cc.warc;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ExtractorCLI {

    private final static Path defaultOutput = Paths.get("output");

    private static Options getOptions() {
        Options options = new Options();
        options.addOption("w", "warc", true, "warc file or directory containing warc files");
        options.addOption("o", "output", true, "output directory");
        options.addOption("ih", "include-http-header-mime-pattern", true,
                "pattern to match http-header mimes for inclusion");
        options.addOption("eh", "exclude-http-header-mime-pattern", true,
                "pattern to match http-header mimes for exclusion");
        options.addOption("id", "include-detected-mime-pattern", true,
                "pattern to match detected mimes for inclusion");
        options.addOption("ed", "exclude-detected-mime-pattern", true,
                "pattern to match http-header mimes for exclusion");
        options.addOption("iu", "include-url-extension", true,
                "pattern to match url extensions for inclusion");
        options.addOption("eu", "exclude-url-extension", true,
                "pattern to match url extensions for exclusion");
        options.addOption("die", "default-include-or-exclude", true,
                "if none of the patterns match, include or not (true,false)");
        options.addOption("t", "include-truncated", false,
                "should we include files that the crawler thought were truncated");
        options.addOption("?", "help", false, "this help message");

        return options;
    }

    private static ExtractorConfig buildConfig(String[] args)
            throws ParseException {
        CommandLineParser cliParser = new GnuParser();
        CommandLine line = cliParser.parse(getOptions(), args);
        Path warcPath = null;
        Path outDir = null;
        if (line.hasOption('w')) {
            warcPath = Paths.get(line.getOptionValue('w'));
        }
        if (line.hasOption('o')) {
            outDir = Paths.get(line.getOptionValue('o'));
        }
        if (warcPath == null) {
            throw new IllegalArgumentException("Must specify warc path: -w");
        }
        if (outDir == null) {
            throw new IllegalArgumentException("Must specify output directory: -o");
        }
        ExtractorConfig config = new ExtractorConfig(warcPath, outDir);

        if (line.hasOption("ih")) {
            Pattern p = Pattern.compile(line.getOptionValue("ih"));
            config.setIncludeHttpMimePattern(p);
        }

        if (line.hasOption("id")) {
            Pattern p = Pattern.compile(line.getOptionValue("id"));
            config.setIncludeDetectedMimePattern(p);
        }

        if (line.hasOption("iu")) {
            Pattern p = Pattern.compile(line.getOptionValue("iu"));
            config.setIncludeExtensionPattern(p);
        }

        if (line.hasOption("eh")) {
            Pattern p = Pattern.compile(line.getOptionValue("eh"));
            config.setExcludeHttpMimePattern(p);
        }
        if (line.hasOption("ed")) {
            Pattern p = Pattern.compile(line.getOptionValue("ed"));
            config.setExcludeDetectedMimePattern(p);
        }
        if (line.hasOption("eu")) {
            Pattern p = Pattern.compile(line.getOptionValue("eu"));
            config.setExcludeExtensionPattern(p);
        }

        if (line.hasOption("die")) {
            String v = line.getOptionValue("die");
            if (v != null && v.equalsIgnoreCase("true")) {
                config.setDefaultInclude(true);
            } else {
                config.setDefaultInclude(false);
            }
        }

        if (line.hasOption("t")) {
            config.setIncludeTruncated(true);
        }
        return config;
    }

    public static void main(String[] args) throws Exception {
        ExtractorConfig config = null;
        if (args.length == 1) {
            config = new ExtractorConfig(Paths.get(args[0]), defaultOutput);
        } else {
            config = buildConfig(args);
        }
        Extractor ex = new Extractor(config);
        ExtractorStats stats = null;
        try {
            stats = ex.execute();
        } catch (Exception e) {
            stats = ex.getExtractorStats();
            e.printStackTrace();
        }
        System.out.println(stats);

    }
}
