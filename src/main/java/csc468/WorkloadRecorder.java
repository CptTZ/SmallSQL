package csc468;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkloadRecorder {
    private static final Logger logger = Logger.getLogger(WorkloadRecorder.class.getName());
    private static WorkloadRecorder INSTANCE;
    private FileWriter fileWriter;

    /**
     * Private constructor to enforce singleton usage.
     */
    private WorkloadRecorder() {
        try {
            var workload = new File("./src/main/java/csc468/workload.txt");
            this.fileWriter = new FileWriter(workload);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    /**
     * Get singleton instance of the workload recorder.
     *
     * @return Workload recorder instance.
     */
    public static WorkloadRecorder getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new WorkloadRecorder();
        }
        return INSTANCE;
    }

    /**
     * Writes a query to the workload file.
     *
     * @param query Valid query as a string to be written.
     * @throws IOException Thrown when FileWriter cannot write to the file.
     */
    public void writeQueryToWorkload(String query) throws IOException {
        if (this.fileWriter == null) {
            logger.log(Level.WARNING, "FileWriter not instantiated.");
        } else {
            this.fileWriter.write(query + "\n\n");
        }
    }

    /**
     * Close the FileWriter.
     *
     * @throws IOException Thrown when FileWriter cannot close.
     */
    public void closeWriter() throws IOException {
        this.fileWriter.close();
    }
}
