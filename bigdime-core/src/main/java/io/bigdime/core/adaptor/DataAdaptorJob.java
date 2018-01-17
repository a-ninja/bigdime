/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.adaptor;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.commons.AdaptorLogger;
import org.quartz.*;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Job that actually invokes the start method on Adaptor.
 * 
 * @author Neeraj Jain
 *
 */
//@Controller
@RestController
@EnableAutoConfiguration
@EnableJpaRepositories
@ComponentScan("io.bigdime")
//@Configuration
@ImportResource({ "classpath*:application-context.xml", "classpath*:META-INF/application-context.xml" })

@Component
@DisallowConcurrentExecution
public class DataAdaptorJob implements Job {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataAdaptorJob.class));
	private Trigger trigger;
	private Scheduler scheduler;

	public DataAdaptorJob() {
		logger.info("DataAdaptorJob constructor", "DataAdaptorJob");
	}

	@RequestMapping("/")
	String getName() {
		return "DataAdaptorJob";
	}

	/**
	 * This method is invoked by the scheduler. For streaming adaptors, this
	 * method will be executed once. For batch adaptors, this wil be executed
	 * based on the cron schedule.
	 */
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		try {
			logger.info("starting adaptor", "executing scheduled job");
			Thread.currentThread().setName("DataAdaptorJob_execute");
			Adaptor dataAdaptor = (Adaptor) context.getJobDetail().getJobDataMap().get("dataAdaptor");
			dataAdaptor.start();
		} catch (DataAdaptorException ex) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER, ex.getMessage(), ex);
			throw new JobExecutionException(ex);
		}
	}

	/**
	 * Schedule the job for an adaptor of batch type. The name of the job is set
	 * to adaptor-batch-scheduler.
	 * 
	 * @param dataAdaptor
	 *            Adaptor on which the start method will be invoked
	 * @param config
	 * @throws SchedulerException
	 */
	public void scheduleBatchJob(Adaptor dataAdaptor) throws SchedulerException {
		logger.info("scheduling job", "scheduling batch job, dataAdaptor=\"{}\"", dataAdaptor);

		ScheduleBuilder<CronTrigger> scheduleBuilder = CronScheduleBuilder
				.cronSchedule(dataAdaptor.getAdaptorConfig().getCronExpression());
		getScheduler(dataAdaptor, "adaptor-batch-scheduler", scheduleBuilder);
	}

	public void scheduleStreamingJob(Adaptor dataAdaptor) throws SchedulerException {
		logger.info("scheduling job", "scheduling streaming job, dataAdaptor=\"{}\"", dataAdaptor);
		SimpleScheduleBuilder simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule();
		getScheduler(dataAdaptor, "adaptor-streaming-scheduler", simpleScheduleBuilder);
	}

	private void getScheduler(Adaptor dataAdaptor, String schedulerName,
			ScheduleBuilder<? extends Trigger> scheduleBuilder) throws SchedulerException {
		JobDetail adaptorJob = JobBuilder.newJob(DataAdaptorJob.class)
				.withIdentity(dataAdaptor.getAdaptorContext().getAdaptorName(), "bigdime.io-job").build();
		adaptorJob.getJobDataMap().put("dataAdaptor", dataAdaptor);
		trigger = TriggerBuilder.newTrigger()
				.withIdentity(dataAdaptor.getAdaptorContext().getAdaptorName(), "bigdime.io-trigger")
				.withSchedule(scheduleBuilder).build();

		final SimpleThreadPool threadPool = new SimpleThreadPool(1, Thread.NORM_PRIORITY);
		threadPool.initialize();
		JobStore jobStore = new RAMJobStore();
		DirectSchedulerFactory.getInstance().createScheduler(schedulerName, DirectSchedulerFactory.DEFAULT_INSTANCE_ID,
				threadPool, jobStore);
		scheduler = DirectSchedulerFactory.getInstance().getScheduler(schedulerName);
		scheduler.start();
		scheduler.scheduleJob(adaptorJob, trigger);
	}

	public static void main(String[] args) {
		SpringApplication.run(DataAdaptorJob.class);
	}

}
