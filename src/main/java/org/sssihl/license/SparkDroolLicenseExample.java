package org.sssihl.license;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

import java.util.ArrayList;
import java.util.List;

/*
Stateless drools and spark integration with license example.
 */
public class SparkDroolLicenseExample {


    public static void main(String[] args){
        /*---------------------------------------------------------------------*/
        //Logging the spark info.
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        //Set Spark Configurations
        SparkConf sparkConf= new SparkConf().setAppName("License Validator").setMaster("local[2]");
        //Set JavaSparkContext.
        JavaSparkContext sparkContext= new JavaSparkContext(sparkConf);

        //Create a Kiebase by loading rule in to the knowledge base.
        //Broadcast the Kiebase so the all the worker node will have the kiebase with them.
        KieServices kieServices= KieServices.Factory.get();
        KieContainer kieContainer= kieServices.getKieClasspathContainer();
        KieBase kieBase= kieContainer.getKieBase();

        //Create a broadcast variable in spark context.
        final Broadcast<KieBase> kieBaseBroadcast= sparkContext.broadcast(kieBase);

        List<Applicant> approvedApplicants= sparkContext.textFile("/home/murali/data.txt").map(s -> {
            String[] data= s.split(",");
            Applicant applicant= new Applicant(data[0], Integer.valueOf(data[1]));
            StatelessKieSession statelessKieSession= kieBaseBroadcast.getValue().newStatelessKieSession();
            statelessKieSession.execute(applicant);
            return applicant;
        }).filter(applicant -> applicant.isValid()).collect();

        for (Applicant applicant: approvedApplicants)
            System.out.println("Applicant Name: " + applicant.getName() + " : Age: " + applicant.getAge() + " eligible for applying license.");
    }
}
