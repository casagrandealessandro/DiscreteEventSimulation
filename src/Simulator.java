import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;

/**
 * @author Alessandro Casagrande - 2066716
 * @version 1.2 2023/12/31
 */
public class Simulator {
    static int selectedServerNumber = -1;

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
            System.out.println("Specified file does not exist! Quitting execution...");
            return;
        }
        Scanner line = new Scanner(inFile.nextLine());
        line.useDelimiter(",");
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
        int i = 0;
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
        // first line output print
        System.out.println(K + "," + H + "," + N + "," + R + "," + (P ? 1 : 0));
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
        // --------------------------------------------------------------------
        // ----------------------------------------- Statistics Data Structures
        // End Times array, one per run
        double[] endTimes = new double[R];
        // Queuing Times array, one per job
        double[] queuingTimes = new double[N];
        int n = 0;
        // Average Queuing Times array, one per run
        double[] avgQueuingTimes = new double[R];
        // Queueing Times matrix, divided per Category
        double[][] queuingTimesCategory = new double[H][N];
        // Average Queuing Times matrix per Category, one column per run
        double[][] avgQueuingTimesCategory = new double[H][R];
        // counter array for the number of Events of each Category
        double[] cntCatJobs = new double[H];
        // Service Times matrix, divided per Category
        double[][] serviceTimesCategory = new double[H][N];
        // Average Service Times matrix per Category, one column per run
        double[][] avgServiceTimesCategory = new double[H][R];
        // --------------------------------------------------------------------
        // --------------------------------------------------------- Simulation
        // R repetitions
        for (int r = 0; r < R; r++) {
            // clearing/resetting structures | '-1' means "no data registered"
            timeLine.clear();
            for (int c = 0; c < H; c++) {
                for (int s = 0; s < N; s++) {
                    queuingTimesCategory[c][s] = -1.0;
                    serviceTimesCategory[c][s] = -1.0;
                }
            }
            // generating the arrival Event of the first Job of each Category
            for (i = 0; i < H; i++) {
                Job firstJob = new Job(categories[i]);
                Event firstEvent = new Event(
                        false,
                        categories[i].getInterarrivalTime(),
                        firstJob);
                firstJob.setAssociatedEvent(firstEvent);
                timeLine.add(firstEvent);
            }
            // here starts the simulation. Below an explanation of how it works:
            //
            // while(timeline has Events to manage)
            // | newEvent ← timeline.poll()
            // | if (newEvent is 'arrival of a Job') then
            // | | process arrival
            // | | schedule next arrival
            // | | select Server
            // | | if (server available) then
            // | | | execute Job
            // | | | schedule end
            // | | else add Job to Server's queue
            // | else
            // | | process end Event
            // | | manage Server's queue and executions
            //
            // meanwhile it'll updates all the statistics needed
            i = 0;
            while (!timeLine.isEmpty()) {
                Event newEvent = timeLine.poll();
                // if it is the case, prints the event extracted (for 2N times)
                if (R == 1 && N <= 10 && P == false && (i < N || newEvent.getEventType() == true)) {
                    System.out.println(newEvent.getArrivalTime() + ","
                            + (newEvent.getEventType() ? newEvent.getAssociatedJob().getServiceTime() : 0.0) + ","
                            + newEvent.getAssociatedJob().getCategory().getCategoryNumber());
                }
                if (newEvent.getEventType() == false && i < N) { // new Job arrival
                    Category eCategory = newEvent.getAssociatedJob().getCategory();
                    // updating counter
                    cntCatJobs[eCategory.getCategoryNumber()]++;
                    // schedule next arrival
                    Job eventJob = new Job(eCategory);
                    Event nextEvent = new Event(
                            false,
                            newEvent.getArrivalTime() + eCategory.getInterarrivalTime(),
                            eventJob);
                    eventJob.setAssociatedEvent(nextEvent);
                    timeLine.add(nextEvent);
                    // select Server according to the wanted scheduling policy
                    Server eServer;
                    if (P == false) {
                        eServer = servers[roundRobin(K)];
                    } else {
                        eServer = null; // delete this line
                        // TODO use custom scheduling policy
                    }
                    if (eServer.getJobInExecution() == null) { // Server available
                        newEvent.getAssociatedJob().setServiceTime(eCategory.getServiceTime());
                        eServer.setJobInExecution(newEvent.getAssociatedJob());
                        // no time in queue
                        queuingTimes[n++] = 0.0;
                        int nCat = newEvent.getAssociatedJob().getCategory().getCategoryNumber();
                        int kc = 0;
                        while (kc < N - 1 && queuingTimesCategory[nCat][kc] != -1.0) {
                            kc++;
                        }
                        queuingTimesCategory[nCat][kc] = 0.0;
                        // updating service time stats
                        kc = 0;
                        while (kc < N - 1 && serviceTimesCategory[nCat][kc] != -1.0) {
                            kc++;
                        }
                        serviceTimesCategory[nCat][kc] = newEvent.getAssociatedJob().getServiceTime();
                        // schedule execution end
                        Job newEventJob = newEvent.getAssociatedJob();
                        Event nextEndEvent = new Event(
                                true,
                                newEvent.getArrivalTime()
                                        + newEvent.getAssociatedJob().getServiceTime(),
                                newEventJob);
                        newEventJob.setAssociatedEvent(nextEndEvent);
                        timeLine.add(nextEndEvent);
                    } else { // Server unavailable
                        // add Job to Server's FIFO Queue
                        eServer.getWaitingJobs().add(newEvent.getAssociatedJob());
                    }
                    // updates the counter of Jobs managed (until N)
                    i++;
                } else if (newEvent.getEventType() == true) { // Job execution end
                    // scheduled server's search
                    int k = 0;
                    while (k < K && servers[k].getJobInExecution() != newEvent.getAssociatedJob()) {
                        k++;
                    }
                    if (!servers[k].getWaitingJobs().isEmpty()) { // queue not empty
                        // executing the first Job in the queue
                        Job nextJob = servers[k].getWaitingJobs().poll();
                        nextJob.setServiceTime(nextJob.getCategory().getServiceTime());
                        servers[k].setJobInExecution(nextJob);
                        // updating queuing time of this new executing job
                        Event jobArrivalEvent = servers[k].getJobInExecution().getAssociatedEvent();
                        queuingTimes[n++] = newEvent.getArrivalTime() - jobArrivalEvent.getArrivalTime();
                        // updating queueing time per category
                        int nCat = nextJob.getCategory().getCategoryNumber();
                        int kc = 0;
                        while (kc < N - 1 && queuingTimesCategory[nCat][kc] != -1.0) {
                            kc++;
                        }
                        queuingTimesCategory[nCat][kc] = newEvent.getArrivalTime() - jobArrivalEvent.getArrivalTime();
                        // updating service time stats
                        kc = 0;
                        while (kc < N - 1 && serviceTimesCategory[nCat][kc] != -1.0) {
                            kc++;
                        }
                        serviceTimesCategory[nCat][kc] = nextJob.getServiceTime();
                        // schedule execution end
                        Job nextEventJob = servers[k].getJobInExecution();
                        Event nextEndEvent = new Event(
                                true,
                                newEvent.getArrivalTime()
                                        + servers[k].getJobInExecution().getServiceTime(),
                                nextEventJob);
                        nextEventJob.setAssociatedEvent(nextEndEvent);
                        timeLine.add(nextEndEvent);
                    } else { // queue empty
                        // no more execution for now
                        servers[k].setJobInExecution(null);
                        // candidate end time for the r-th run
                        endTimes[r] = newEvent.getArrivalTime();
                    }
                } else {
                    // new Job arrival, but i ≥ N
                }
            }
            // calculating the average queuing time of this run
            for (n = 0; n < N; n++) {
                avgQueuingTimes[r] += queuingTimes[n];
            }
            avgQueuingTimes[r] /= N;
            n = 0;
            // calculating the average queuing times per category of this run
            for (int h = 0; h < H; h++) {
                int k = 0;
                while (k < N - 1 && queuingTimesCategory[h][k] != -1.0) {
                    avgQueuingTimesCategory[h][r] += queuingTimesCategory[h][k];
                    k++;
                }
                avgQueuingTimesCategory[h][r] /= (k != 0 ? k : 1);
            }
            // calculating the average service times per category of this run
            for (int h = 0; h < H; h++) {
                int k = 0;
                while (k < N - 1 && serviceTimesCategory[h][k] != -1.0) {
                    avgServiceTimesCategory[h][r] += serviceTimesCategory[h][k];
                    k++;
                }
                avgServiceTimesCategory[h][r] /= (k != 0 ? k : 1);
            }
        }
        // --------------------------------------------------------------------
        // --------------------------------------------- Remaining Output Print
        System.out.println(ET(R, endTimes));
        System.out.println(AQT_all(R, avgQueuingTimes));
        for (int h = 0; h < H; h++) {
            System.out.println(cntCatJobs[h] / R + "," + AQT(R, avgQueuingTimesCategory, h)
                    + "," + AST(R, avgServiceTimesCategory, h));
        }
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

    /**
     * This function calculates the average of a given array of end times.
     * 
     * @param R        the number of end times (number or repetitions)
     * @param endTimes an array of end times, one value for each repetition
     * @return the average of the end times.
     */
    private static double ET(int R, double[] endTimes) {
        double sumEndTime = 0;
        for (int r = 0; r < R; r++) {
            sumEndTime += endTimes[r];
        }
        return sumEndTime / R;
    }

    /**
     * This function calculates the average queuing time of a Job
     * 
     * @param R               the number of repetitions
     * @param avgQueuingTimes an array of
     *                        double values representing the average queuing times
     *                        for each repetition.
     *                        The length of the array is R, which is the total
     *                        number of repetition.
     * @return the total average queuing time for all repetitions
     */
    private static double AQT_all(int R, double[] avgQueuingTimes) {
        double sumAvgTime = 0;
        for (int r = 0; r < R; r++) {
            sumAvgTime += avgQueuingTimes[r];
        }
        return sumAvgTime / R;
    }

    /**
     * The function calculates the average queuing time for a specific category
     * based on the given average queuing times and the number of repetitions
     * 
     * @param R                       the number of repetitions
     * @param avgQueuingTimesCategory matrix of doubles. It represents the average
     *                                queuing times for
     *                                different Categories throughout each
     *                                repetition. One Category each row, the average
     *                                queuing time for a repetition each column
     * @param nCat                    the category number. It is used
     *                                to access the specific row in the
     *                                "avgQueuingTimesCategory" matrix
     * @return the total average queuing time for a specific
     *         category.
     */
    private static double AQT(int R, double[][] avgQueuingTimesCategory, int nCat) {
        double sumAvgTime = 0;
        for (int r = 0; r < R; r++) {
            sumAvgTime += avgQueuingTimesCategory[nCat][r];
        }
        return sumAvgTime / R;
    }

    /**
     * The function calculates the average service time for a specific category
     * based on the given average service times and the number of repetitions
     * 
     * @param R                       the number of repetitions
     * @param avgServiceTimesCategory matrix of doubles. It represents the average
     *                                service times for different Categories
     *                                throughout each repetition. One Category each
     *                                row, the average service time for a repetition
     *                                each column
     * @param nCat                    the category number. It is used to access
     *                                the specific row in the
     *                                "avgServiceTimesCategory" matrix
     * @return the total average service time for a specific category.
     */
    private static double AST(int R, double[][] avgServiceTimesCategory, int nCat) {
        double sumAvgTime = 0;
        for (int r = 0; r < R; r++) {
            sumAvgTime += avgServiceTimesCategory[nCat][r];
        }
        return sumAvgTime / R;
    }
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
        this.arrivalTime = arrivalTime;
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
    private Event associatedEvent;

    /**
     * This constructor initializes a new Job object with the given parameters
     * 
     * @param category    category to which the Job belongs
     * @param serviceTime the amount of time required by the Job to be executed
     */
    public Job(Category category) {
        this.category = category;
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

    /**
     * Setter for the service time field
     * 
     * @param serviceTime the service time assignated to this Job
     */
    public void setServiceTime(double serviceTime) {
        this.serviceTime = serviceTime;
    }

    /**
     * Getter for the associated Event field
     * 
     * @return the link "associatedEvent" as an Event reference
     */
    public Event getAssociatedEvent() {
        return this.associatedEvent;
    }

    /**
     * Setter for the associated Event field
     * 
     * @param associatedEvent an Event reference
     */
    public void setAssociatedEvent(Event associatedEvent) {
        this.associatedEvent = associatedEvent;
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