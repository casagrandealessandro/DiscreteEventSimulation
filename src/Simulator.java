import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;

/**
 * @author Alessandro Casagrande - 2066716
 * @version 1.0 2023/12/10
 */
public class Simulator {
    public static void main(String[] args) {
        // Events keep sorted by thei arrival time
        PriorityQueue<Event> timeLine = new PriorityQueue<Event>();
        /*
         * K = # of Servers
         * H = # of categories
         * N = # of Jobs to be simulated
         * R = # of repetition of the simulation
         * P = scheduling policy to be used
         */
        int[] simulationParam = new int[5];
        // --------------------------------------------------------------------
        // -------------------------------------------------- reading from file
        String inFilePath = args[0];
        Scanner inFile = null;
        try {
            inFile = new Scanner(new FileReader(inFilePath));
        } catch (FileNotFoundException e) {
            System.out.println("Specified file does not exist! Quitting execution...");
            return;
        }
        Scanner line = new Scanner(inFile.nextLine());
        line.useDelimiter(",");
        int i = 0;
        // reading the first line
        while (line.hasNext()) {
            simulationParam[i++] = Integer.parseInt(line.next());
        }
        // H lines, 4 values each
        double[][] generationParam = new double[simulationParam[1]][4];
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
        // ------------------------------------------------------ first setting
        Category categories[] = new Category[simulationParam[1]];
        // populatin all the Categories with their configuration parameters
        for (i = 0, j = 0; i < simulationParam[1]; i++, j = 0) {
            categories[i] = new Category(
                    i,
                    generationParam[i][j++],
                    generationParam[i][j++],
                    (long) (generationParam[i][j++]),
                    (long) (generationParam[i][j++]));
        }
        // simulating the arrival of the first Job of each Category
        for (i = 0; i < simulationParam[1]; i++) {
            timeLine.add(new Event(false, new Job(
                    categories[i],
                    categories[i].getInterarrivalTime(),
                    categories[i].getServiceTime())));
        }
        // --------------------------------------------------------------------
        // --------------------------------------------- TODO manage simulation
    }
}

// =============================================================================
// OTHER CLASSES
// =============================================================================

/**
 * The Event class represents a simulation event with a boolean eventType and
 * an associated Job, and implements the Comparable interface to compare events
 * based on the arrival time of their associated jobs.
 */
class Event implements Comparable<Event> {
    public boolean eventType;
    private Job associatedJob;

    /**
     * This constructor initializes an Event object with the given parameters
     * 
     * @param eventType     type of the event: false if the event represents the
     *                      arrival of a new Job, true if it represents the end
     *                      of the execution of a Job currently in execution
     * @param associatedJob the Job to which the event relates
     */
    public Event(boolean eventType, Job associatedJob) {
        this.eventType = eventType;
        this.associatedJob = associatedJob;
    }

    /**
     * The compareTo function compares two Events based on their associated Jobs
     * 
     * @param e an object of the Event class with which to compare
     * @return an integer value: LESS than zero if the associated Job of this
     *         Event comes BEFORE than the associated Job of the given Event;
     *         GREATER than zero if the associated Job of this Event comes
     *         AFTER than the associated Job of the given Event; ZERO if the
     *         associated Jobs arrive at the SAME moment
     */
    @Override
    public int compareTo(Event e) {
        return this.associatedJob.compareTo(e.associatedJob);
    }
}

// =============================================================================

/**
 * The Job class represents a job belonging to a category and implements the
 * Comparable interface to compare jobs based on their arrival times.
 */
class Job implements Comparable<Job> {
    private Category category;
    private double arrivalTime;
    private double serviceTime;

    /**
     * This constructor initializes a new Job object with the given parameters
     * 
     * @param category    category to which the Job belongs
     * @param arrivalTime the arrival time of the Job
     * @param serviceTime the amount of time required by the Job to be executed
     */
    public Job(Category category, double arrivalTime, double serviceTime) {
        this.category = category;
        this.arrivalTime = arrivalTime;
        this.serviceTime = serviceTime;
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
     * Getter for the service time field
     * 
     * @return the value of the "serviceTime" field as a double
     */
    public double getServiceTime() {
        return this.serviceTime;
    }

    /**
     * The function compares the arrival time of two Job objects
     * 
     * @param j an object of the Job class with which to compare
     * @return an integer value: LESS than zero if the arrival time of this Job
     *         is LESS than the one given; GREATER than zero if the arrival time
     *         of this Job is GREATER than the one given; ZERO if arrival time
     *         of the two Jobs are equals
     */
    @Override
    public int compareTo(Job j) {
        if (this.arrivalTime < j.arrivalTime)
            return -1;
        else if (this.arrivalTime > j.arrivalTime)
            return 1;
        else
            return 0;
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
    }
}

// =============================================================================