import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jasypt.util.text.BasicTextEncryptor;

import java.io.*;

public class HDFSUploader {

    private static final String HDFS_URI = "hdfs://localhost:9000";

    public static void main(String[] args) {
        // Replace "ankush.txt" with the path to your file
        String filePath = "/home/ankushtiwari/Desktop/CloudProject/ankush.txt";

        // Encrypt the file
        String encryptedFilePath = encryptFile(filePath);

        // Upload the encrypted file to HDFS
        uploadToHDFS(encryptedFilePath, "/CloudProject/Input");
    }

    private static String encryptFile(String filePath) {
        // Initialize Jasypt text encryptor
        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        // Set your encryption password here
        textEncryptor.setPassword("ankushtiwari");

        // Read file content and encrypt it
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            String encryptedContent = textEncryptor.encrypt(sb.toString());
            String encryptedFilePath = filePath + ".encrypted";
            // Write encrypted content to a new file
            try (PrintWriter writer = new PrintWriter(encryptedFilePath)) {
                writer.write(encryptedContent);
            }
            return encryptedFilePath;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void uploadToHDFS(String localFilePath, String hdfsFilePath) {
        try {
            // Set Hadoop configuration
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", HDFS_URI);

            // Initialize Hadoop file system
            FileSystem fs = FileSystem.get(conf);

            // Upload file to HDFS
            Path srcPath = new Path(localFilePath);
            Path destPath = new Path(HDFS_URI + hdfsFilePath);
            fs.copyFromLocalFile(srcPath, destPath);

            // Close file system
            fs.close();
            System.out.println("File uploaded to HDFS successfully and its going to be encrypted.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
	
