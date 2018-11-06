package org.tallison.cc;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CopyRefetchedToMainRepo {
    public static void main(String[] args) throws Exception {
        Path refetchedRoot = Paths.get(args[0]);
        Path mainRepoRoot = Paths.get(args[1]);
        for (File dir : refetchedRoot.toFile().listFiles()) {
            for (File f : dir.listFiles()) {
                String digest = f.getName();
                Path newTarg = mainRepoRoot.resolve(digest.substring(0,2)+"/"+digest);
                if (!Files.exists(newTarg)) {
                    try {
                        Files.move(f.toPath(), newTarg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
