package org.tallison.cc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class RefetchedDeleter {
    //read through the refetch tables.  Delete some percent of
    //the truncated files from the original doc root.
    //If the file couldn't be refetched or if rand() is > percentage
    //to delete, copy the truncated file to the truncated directory.

    public static void main(String[] args) throws Exception {
        Path refetchTableDir = Paths.get(args[0]);
        Path origDocsRoot = Paths.get(args[1]);
        Path truncatedDocsRoot = Paths.get(args[2]);
        float percentToDelete = Float.parseFloat(args[3]);
        Random r = new Random();
        long savedBytes = 0;
        for (File f : refetchTableDir.toFile().listFiles()) {
            BufferedReader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
            String line = reader.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                String url = cols[0];
                String origDigest = cols[1];
                String newDigest = cols[2];
                String status = cols[3];
                long length = Long.parseLong(cols[4]);
                Path origFile = origDocsRoot.resolve(origDigest.substring(0,2)+"/"+origDigest);

                if ("SUCCESS".equals(status) && r.nextFloat() <= percentToDelete) {
                    Path toDelete = origDocsRoot.resolve(origDigest.substring(0,2)+"/" +origDigest);
                    try {
                        Files.delete(toDelete);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else {
                    Path truncatedTarget = truncatedDocsRoot.resolve(origDigest.substring(0,2)+"/"+origDigest);
                    System.out.println("copying to truncated: "+truncatedTarget);
                    if (! Files.isDirectory(truncatedTarget.getParent())) {
                        Files.createDirectories(truncatedTarget.getParent());
                    }
                    Files.copy(origFile, truncatedTarget);
                }
                line = reader.readLine();
            }

        }

    }
}
