package edu.ismagi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;

public class HadoopFileStatus {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: HadoopFileStatus <directory> <source_file> <target_file>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs;

        try {
            fs = FileSystem.get(conf);

            
            String directory = args[0];
            String sourceFile = args[1];
            String targetFile = args[2];

            Path filepath = new Path(directory, sourceFile);
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.exit(1);
            }

            // Display file info
            FileStatus status = fs.getFileStatus(filepath);
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen());
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());

            // Print block info
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Rename file
            Path targetPath = new Path(directory, targetFile);
            fs.rename(filepath, targetPath);
            System.out.println("File renamed to: " + targetPath.getName());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
