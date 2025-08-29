package joshua.green.data;

import org.cloudsimplus.cloudlets.Cloudlet;

/**
 * Description:
 * Author: joshua
 * Date: 2025/4/10
 */
public class TimedCloudlet {
    private final Cloudlet cloudlet;
    private final double submissionTime;

    public TimedCloudlet(double submissionTime, Cloudlet cloudlet) {
        this.cloudlet = cloudlet;
        this.submissionTime = submissionTime;
    }

    public Cloudlet getCloudlet() { return cloudlet; }
    public double getSubmissionTime() { return submissionTime; }
}
