import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;

/**
 * @author Alessandro Casagrande - 2066716
 * @version 1.1 2023/12/13
 */
public class Simulator {
    static int selectedServerNumber = 0;

    public static void main(String[] args) {
        // Events keep sorted by their arrival time
        PriorityQueue<Event> timeLine = new PriorityQueue<Event>();
        // --------------------------------------------------------------------
        // -------------------------------------------------- Reading from file
        String inFilePath = args[0];
        Scanner inFile = null;
        try {
            inFile = new Scanner(new FileReader(inFilePath));
        } catch (FileNotFoundException e) {
            System.out.println("Specified file does not exist!"
                    + " Quitting execution...");
            return;
        }
        Scanner line = new Scanner(inFile.nextLine());
        line.useDelimiter(",");
        int i = 0;
        // reading the first line
        /*
         * K = # of Servers
         * H = # of categories
         * N = # of Jobs to be simulated
         * R = # of repetition of the simulation
         * P = scheduling policy to be used: true ≡ round-robin; false ≡ custom
         */
        final int K = Integer.parseInt(line.next());
        final int H = Integer.parseInt(line.next());
        final int N = Integer.parseInt(line.next());
        final int R = Integer.parseInt(line.next());
        final boolean P = Boolean.parseBoolean(line.next());
        // H lines, 4 values each
        double[][] generationParam = new double[H][4];
        int j;
        i = 0;
        // reading all the 2+r lines, 0 ≤ r < H
        while (inFile.hasNextLine()) {
            line = new Scanner(inFile.nextLine());
            line.useDelimiter(",");
            j = 0;
            // reading the 4 values of this line
            while (line.hasNext()) {
                generationParam[i][j++] = Double.parseDouble(line.next());
            }
            i++;
        }
        // --------------------------------------------------------------------
        // ------------------------------------------------------ First setting
        Category categories[] = new Category[H];
        // populating all the Categories with their configuration parameters
        for (i = 0, j = 0; i < H; i++, j = 0) {
            categories[i] = new Category(
                    i,
                    generationParam[i][j++],
                    generationParam[i][j++],
                    (long) (generationParam[i][j++]),
                    (long) (generationParam[i][j++]));
        }
        // creating the Servers instances
        Server[] servers = new Server[K];
        for (i = 0; i < K; i++) {
            servers[i] = new Server(i);
        }
        // simulating the arrival of the first Job of each Category
        for (i = 0; i < H; i++) {
            timeLine.add(new Event(
                    false,
                    categories[i].getInterarrivalTime(),
                    new Job(categories[i], categories[i].getServiceTime())));
        }
        // --------------------------------------------------------------------
        // --------------------------------------------------------- Simulation
        // R repetitions
        for (int r = 0; r < R; r++) {
            // N - H iterations (total of N Jobs simulated)
            i = H;
            boolean finished = false;
            while (!finished) {
                Event newEvent = timeLine.poll();
                if (newEvent.getEventType() == false && i < N) { // new Job arrival
                    Category eCategory = newEvent.getAssociatedJob().getCategory();
                    // schedule next arrival
                    timeLine.add(new Event(
                            false,
                            newEvent.getArrivalTime() + eCategory.getInterarrivalTime(),
                            new Job(eCategory, eCategory.getServiceTime())));
                    // select Server according to the wanted scheduling policy
                    Server eServer;
                    if (P == false) {
                        eServer = servers[roundRobin(K)];
                    } else {
                        eServer = null; // delete this line
                        // TODO use custom scheduling policy
                    }
                    if (eServer.getJobInExecution() == null) { // Server available
                        eServer.setJobInExecution(newEvent.getAssociatedJob());
                        // schedule execution end
                        timeLine.add(new Event(
                                true,
                                newEvent.getArrivalTime()
                                        + newEvent.getAssociatedJob().getServiceTime(),
                                newEvent.getAssociatedJob()));
                    } else { // Server unavailable
                        // add Job to Server's FIFO Queue
                        eServer.getWaitingJobs().add(newEvent.getAssociatedJob());
                    }
                    i++;
                } else if (newEvent.getEventType() == true) { // Job execution end
                    // scheduled server's search
                    int k = 0;
                    while (k < K && servers[k].getJobInExecution() != newEvent.getAssociatedJob()) {
                        k++;
                    }
                    if (!servers[k].getWaitingJobs().isEmpty()) { // queue not empty
                        // executing the first Job in the queue
                        servers[k].setJobInExecution(servers[k].getWaitingJobs().poll());
                        // schedule execution end
                        timeLine.add(new Event(
                                true,
                                newEvent.getArrivalTime()
                                        + servers[k].getJobInExecution().getServiceTime(),
                                servers[k].getJobInExecution()));
                    } else { // queue empty
                        // no more execution for now
                        servers[k].setJobInExecution(null);
                    }
                    // TODO update stats
                } else { // new Job arrival, but i ≥ N
                }
                // checking is the simulation is finished
                finished = true;
                for (int k = 0; k < K; k++) {
                    if (servers[k].getJobInExecution() != null)
                        finished = false;
                }
            }
        }

        // TODO print output
    }

    /**
     * The roundRobin function selects the next server number using
     * round-robin scheduling policy
     * 
     * @param K The total number of servers available
     * @return The selected server number
     */
    private static int roundRobin(int K) {
        selectedServerNumber = (selectedServerNumber + 1) % K;
        return selectedServerNumber;
    }

    // TODO develop custom scheduling policy

    // TODO implement statistic methods
    // End Time
    // Average Queueing Time of all jobs
    // Average Queueing Time of job a category
}

// =============================================================================
// OTHER CLASSES
// =============================================================================

/**
 * The Event class represents a simulation event with a boolean eventType and
 * an associated Job, and implements the Comparable interface to compare events
 * based on the arrival time of their associated jobs
 */
class Event implements Comparable<Event> {
    private boolean eventType;
    private double arrivalTime;
    private Job associatedJob;

    /**
     * This constructor initializes an Event object with the given parameters
     * 
     * @param eventType     type of the event: false if the event represents the
     *                      arrival of a new Job, true if it represents the end
     *                      of the execution of a Job currently in execution
     * @param arrivalTime   the arrival time of the event
     * @param associatedJob the Job to which the event relates
     */
    public Event(boolean eventType, double arrivalTime, Job associatedJob) {
        this.eventType = eventType;
        this.associatedJob = associatedJob;
    }

    /**
     * Getter for the event type field
     * 
     * @return the value of "eventType" field as a boolean
     */
    public boolean getEventType() {
        return this.eventType;
    }

    /**
     * Getter for the arrival time field
     * 
     * @return the value of "arrivalTime" field as a double
     */
    public double getArrivalTime() {
        return this.arrivalTime;
    }

    /**
     * Getter for the associated Job field
     * 
     * @return the link "associatedJob" as a Job reference
     */
    public Job getAssociatedJob() {
        return this.associatedJob;
    }

    /**
     * The compareTo function compares two Events based on their arrival time
     * 
     * @param e an object of the Event class with which to compare
     * @return an integer value: LESS than zero if this Event comes BEFORE the
     *         given Event; GREATER than zero if this Event comes AFTER than the
     *         given Event; ZERO if the two Events arrive at the SAME moment
     */
    @Override
    public int compareTo(Event e) {
        if (this.arrivalTime < e.arrivalTime)
            return -1;
        else if (this.arrivalTime > e.arrivalTime)
            return 1;
        else
            return 0;
    }
}

// =============================================================================

/**
 * The Job class represents a job belonging to a category and implements the
 * Comparable interface to compare jobs based on their arrival times
 */
class Job {
    private Category category;
    private double serviceTime;

    /**
     * This constructor initializes a new Job object with the given parameters
     * 
     * @param category    category to which the Job belongs
     * @param serviceTime the amount of time required by the Job to be executed
     */
    public Job(Category category, double serviceTime) {
        this.category = category;
        this.serviceTime = serviceTime;
    }

    /**
     * Getter for the category field
     * 
     * @return the link "category" as a Category reference
     */
    public Category getCategory() {
        return this.category;
    }

    /**
     * Getter for the service time field
     * 
     * @return the value of the "serviceTime" field as a double
     */
    public double getServiceTime() {
        return this.serviceTime;
    }
}

// =============================================================================

/**
 * The Category class represents a category with his own generators and
 * related parameters needed for the random value generations
 */
class Category {
    private int categoryNumber;
    private Random arrivalGenerator;
    private Random serviceGenerator;
    private double lambdaArrival;
    private double lambdaService;

    /**
     * This constructor initializes a new Category object with the given parameters
     * 
     * @param categoryNumber the identifying number of the Category
     * @param lambdaArrival  the parameter for the exponential distribution
     *                       arrival time random generator
     * @param lambdaService  the parameter for the exponential distribution
     *                       service time random generator
     * @param seedArrival    the seed for the arrival time random generator
     * @param seedService    the seed for the service time random generator
     */
    public Category(int categoryNumber, double lambdaArrival,
            double lambdaService, long seedArrival, long seedService) {
        this.categoryNumber = categoryNumber;
        this.lambdaArrival = lambdaArrival;
        this.lambdaService = lambdaService;
        this.arrivalGenerator = new Random(seedArrival);
        this.serviceGenerator = new Random(seedService);
    }

    /**
     * Getter for the category number field
     * 
     * @return the value of "categoryNumber" field as an integer
     */
    public int getCategoryNumber() {
        return this.categoryNumber;
    }

    /**
     * This method calculates and returns a new interarrival time value,
     * generated using exponential distribution
     * 
     * @return a new random value as a double
     */
    public double getInterarrivalTime() {
        double alpha = arrivalGenerator.nextFloat();
        return -(1 / this.lambdaArrival) * Math.log(1 - alpha);
    }

    /**
     * This method calculates and returns a new service time value,
     * generated using exponential distribution
     * 
     * @return a new random value as a double
     */
    public double getServiceTime() {
        double alpha = serviceGenerator.nextFloat();
        return -(1 / this.lambdaService) * Math.log(1 - alpha);
    }
}

// =============================================================================

/**
 * The Server class represents a server, which can have a Job currently in
 * execution and has his own FIFO Queue containing Jobs waiting to be executed
 */
class Server {
    private int serverNumber;
    private Job jobInExecution;
    private Queue<Job> waitingJobs;

    /**
     * This constructor initializes a new Server object with the given parameters
     * 
     * @param serverNumber the identifying number of the Server
     */
    public Server(int serverNumber) {
        this.serverNumber = serverNumber;
        this.waitingJobs = new LinkedList<Job>();
    }

    /**
     * Getter for the Job in execution field
     * 
     * @return the link "jobInExecution" as a Job reference
     */
    public Job getJobInExecution() {
        return this.jobInExecution;
    }

    /**
     * Setter for the Job in execution field
     * 
     * @param jobInExecution Job reference to set "jobInExecution" link
     */
    public void setJobInExecution(Job jobInExecution) {
        this.jobInExecution = jobInExecution;
    }

    /**
     * Getter for the waiting Jobs Queue field
     * 
     * @return the link "waitingJobs" as a Queue<Job> reference
     */
    public Queue<Job> getWaitingJobs() {
        return this.waitingJobs;
    }
}

// =============================================================================