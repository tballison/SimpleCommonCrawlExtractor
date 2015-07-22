package org.tallison.commoncrawl;/*
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.mime.MediaType;


public class ExtractorConfig {

    private final Path warcPath;//file or directory
    private final Path outputDir;
    long maxPayloadBytes = -1;
    Pattern includeHttpMimePattern = null;
    Pattern excludeHttpMimePattern = null;
    Pattern includeDetectedMimePattern = null;
    Pattern excludeDetectedMimePattern = null;
    Pattern includeExtensionPattern = null;
    Pattern excludeExtensionPattern = null;
    int numBytesToSniffForDetection = 1000;
    boolean defaultInclude = false;
    boolean overwrite = false;
    private boolean includeTruncated;

    public ExtractorConfig(Path warcPath, Path outputDir) {
        this.warcPath = warcPath;
        this.outputDir = outputDir;
    }

    public Path getWarcPath() {
        return warcPath;
    }

    public Path getOutputDir() {
        return outputDir;
    }

    public long getMaxPayloadBytes() {
        return maxPayloadBytes;
    }

    public void setMaxPayloadBytes(long maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public Pattern getIncludeHttpMimePattern() {
        return includeHttpMimePattern;
    }

    public void setIncludeHttpMimePattern(Pattern includeHttpMimePattern) {
        this.includeHttpMimePattern = includeHttpMimePattern;
    }

    public Pattern getExcludeHttpMimePattern() {
        return excludeHttpMimePattern;
    }

    public void setExcludeHttpMimePattern(Pattern excludeHttpMimePattern) {
        this.excludeHttpMimePattern = excludeHttpMimePattern;
    }

    public Pattern getIncludeDetectedMimePattern() {
        return includeDetectedMimePattern;
    }

    public void setIncludeDetectedMimePattern(Pattern includeDetectedMimePattern) {
        this.includeDetectedMimePattern = includeDetectedMimePattern;
    }

    public Pattern getExcludeDetectedMimePattern() {
        return excludeDetectedMimePattern;
    }

    public void setExcludeDetectedMimePattern(Pattern excludeDetectedMimePattern) {
        this.excludeDetectedMimePattern = excludeDetectedMimePattern;
    }

    public Pattern getIncludeExtensionPattern() {
        return includeExtensionPattern;
    }

    public void setIncludeExtensionPattern(Pattern includeExtensionPattern) {
        this.includeExtensionPattern = includeExtensionPattern;
    }

    public Pattern getExcludeExtensionPattern() {
        return excludeExtensionPattern;
    }

    public void setExcludeExtensionPattern(Pattern excludeExtensionPattern) {
        this.excludeExtensionPattern = excludeExtensionPattern;
    }

    public int getNumBytesToSniffForDetection() {
        return numBytesToSniffForDetection;
    }

    public void setNumBytesToSniffForDetection(int numBytesToSniffForDetection) {
        this.numBytesToSniffForDetection = numBytesToSniffForDetection;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public boolean isDefaultInclude() {
        return defaultInclude;
    }

    public void setDefaultInclude(boolean defaultInclude) {
        this.defaultInclude = defaultInclude;
    }

    /**
     * If this is all you know so far, do you know enough to exclude this record.
     *
     * @param urlExtension
     * @param httpMediaType
     * @return
     */
    public boolean earlySelectExclude(String urlExtension, MediaType httpMediaType) {

        if (excludeHttpMimePattern != null && httpMediaType != null) {
            Matcher m = excludeHttpMimePattern.matcher(httpMediaType.toString());
            if (m.find())
                return false;
        }

        if (excludeExtensionPattern != null && urlExtension != null) {
            Matcher m = excludeExtensionPattern.matcher(urlExtension);
            if (m.find())
                return false;
        }
        return true;
    }

    public boolean select(String urlExtension, MediaType httpMediaType,
                          MediaType detectedMediaType) {

        if (excludeDetectedMimePattern != null && detectedMediaType != null) {
            Matcher m = excludeDetectedMimePattern.matcher(detectedMediaType.toString());
            if (m.find())
                return false;
        }

        if (excludeHttpMimePattern != null && httpMediaType != null) {
            Matcher m = excludeHttpMimePattern.matcher(httpMediaType.toString());
            if (m.find())
                return false;
        }

        if (excludeExtensionPattern != null && urlExtension != null) {
            Matcher m = excludeExtensionPattern.matcher(urlExtension);
            if (m.find())
                return false;
        }

        if (includeDetectedMimePattern != null && detectedMediaType != null) {
            Matcher m = includeDetectedMimePattern.matcher(detectedMediaType.toString());
            if (m.find()) {
                return true;
            }
        }
        if (includeHttpMimePattern != null && httpMediaType != null) {
            Matcher m = includeHttpMimePattern.matcher(httpMediaType.toString());
            if (m.find()) {
                return true;
            }
        }
        if (includeExtensionPattern != null && urlExtension != null) {
            Matcher m = includeExtensionPattern.matcher(urlExtension);
            if (m.find()) {
                return true;
            }
        }

        //if any includes were specified, and you've made it here
        //do not include this record
        if (includeExtensionPattern != null || includeHttpMimePattern != null ||
                includeExtensionPattern != null) {
            return false;
        }

        return defaultInclude;
    }

    public void setIncludeTruncated(boolean includeTruncated) {
        this.includeTruncated = includeTruncated;
    }

    public boolean isIncludeTruncated() {
        return includeTruncated;
    }
}