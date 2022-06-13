package com.report.service;

import com.report.aws.service.AwsS3Service;
import com.report.utils.email.EmailSender;
import com.report.dto.*;
import com.report.model.ReportRequest;
import com.report.provider.ReportDataProvider;
import com.report.provider.ReportProcessProvider;
import com.report.repository.ReportRequestRepository;
import com.report.security.authentication.AuthenticationService;
import com.report.security.dto.LoggedInUserDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class ReportServiceImpl implements ReportService {

	private static final String emailSubject = "Report %s for request %s is generated";
	private static final ObjectMapper mapper = new ObjectMapper();
	private final AtomicBoolean isActive = new AtomicBoolean();

	@Autowired
	private TaskScheduler taskScheduler;

	@Autowired
	private ReportDataProvider reportDataProvider;

	@Autowired
	private ReportRequestService reportRequestService;

	@Autowired
	@Qualifier("reportExecutor")
	private ThreadPoolTaskExecutor executor;

	@Autowired
	private ReportProcessProvider<Long> reportIdReportProcessProvider;

	@Autowired
	private ReportProcessProvider<ReportRequestDTO> customReportProcessProvider;

	@Autowired
	private AwsS3Service awsS3Service;

	@Autowired
	private EmailSender emailSender;

	@Autowired
	private AuthenticationService authenticationService;

	@Resource
	private ReportRequestRepository reportRequestRepository;

	@Value("${app.report.name}")
	private String appName;

	@Value("${app.report.process.maxExecutorThreads:1}")
	private int maxExecutorThreads;

	@Value("${app.report.from}")
	private String fromEmail;

	@Value("${app.report.aws.url.lifetime:PT24H}")
	private Duration s3UrlLifetime;

	@Value("${app.report.process.enable:true}")
	private Boolean enable;


	@Override
	@SneakyThrows
	public ReportResponseDTO processReport(ReportRequestDTO reportRequestDTO) {
		ReportRequest reportRequest = new ReportRequest();
		LoggedInUserDTO loggedInUserDTO = (LoggedInUserDTO) SecurityContextHolder.getContext().getAuthentication();
		reportRequest.setCreatedBy(loggedInUserDTO.getUserId());
		reportRequest.setLastUpdateBy(loggedInUserDTO.getUserId());
		reportRequest.setReportId(reportRequestDTO.getReportId());
		reportRequest.setReportName(reportRequestDTO.getReportName());
		reportRequest.setClientId(reportRequestDTO.getClientId());
		reportRequest.setAppName(appName);
		reportRequest.setReportFilter(mapper.writeValueAsString(reportRequestDTO.getFilter()));
		reportRequest.setEstimateTimeInSeconds(60L);
		reportRequest.setDownloadFileName(reportRequestDTO.getDownloadFileName());
		String[] emails = CollectionUtils.isNotEmpty(reportRequestDTO.getEmails()) ? reportRequestDTO.getEmails().toArray(new String[0])
				: new String[] {loggedInUserDTO.getEmail()};
		reportRequest.setEmails(emails);
		ReportResponseDTO result = ReportResponseDTO.toResponseDto(reportRequestRepository.save(reportRequest));
		taskScheduler.schedule(this::reportProcessing, Instant.now().plusSeconds(5));
		return result;
	}

	@Override
	public ReportDownloadDTO getDownloadData(ReportRequestDTO reportRequestDTO) {
		return reportDataProvider.getData(reportRequestDTO);
	}

	@Override
	@SneakyThrows
	public DownloadUrlDTO getDownloadUrl(Long requestId) {
		ReportRequest reportRequest = reportRequestRepository.getById(requestId);
		if (StringUtils.isEmpty(reportRequest.getUrlData())) {
			return new DownloadUrlDTO();
		}
		return generateUrl(reportRequest, 120000L);
	}

	@SneakyThrows
	private DownloadUrlDTO generateUrl(ReportRequest reportRequest, Long expirationTimeMs) {
		ReportUrlDTO reportUrlDTO = mapper.readValue(reportRequest.getUrlData(), ReportUrlDTO.class);
		if (StringUtils.isNotEmpty(reportUrlDTO.getFullUrl())) {
			return new DownloadUrlDTO(reportUrlDTO.getFullUrl());
		}
		return new DownloadUrlDTO(awsS3Service.getFileUrl(reportUrlDTO.getAwsBucket(), reportUrlDTO.getAwsFullFilePath(), expirationTimeMs));
	}

	@Scheduled(cron = "${app.report.process.cron:0 0/5 * * * *}")
	public void reportProcessing() {
		if (!enable || !isActive.compareAndSet(false, true)) {
			log.info("Request scheduling is disabled or already started");
			return;
		}
		try {
			int freeThreads = maxExecutorThreads - executor.getActiveCount();
			if (freeThreads <= 0) {
				log.info("No free threads to process report request now. Threads in use {}", executor.getActiveCount());
				return;
			}
			ReportRequestFilterDTO filterDTO = new ReportRequestFilterDTO();
			filterDTO.getApps().add(appName);
			filterDTO.getStatuses().add(ReportRequest.Status.PENDING);
			filterDTO.setRecordsPerPage(freeThreads);
			List<ReportRequest> reportRequestList = reportRequestRepository.findAll(filterDTO);
			reportRequestList.forEach(r -> r.setStatus(ReportRequest.Status.IN_PROGRESS));
			reportRequestRepository.saveAll(reportRequestList);
			log.info("Start processing {} report requests with {} free threads", reportRequestList.size(), freeThreads);
			reportRequestList.forEach(reportRequest ->
					CompletableFuture.runAsync(() -> {
										final var reportRequestDTO = new ReportRequestDTO(reportRequest.getReportId(),
																									   reportRequest.getReportName(),
																									   reportRequest.getReportFilter());
										reportRequestDTO.setDownloadFileName(reportRequest.getDownloadFileName());
										processReport(reportRequestDTO, reportRequest.getId());
									 }, executor)
							.exceptionally(e -> {
								log.error(e.getMessage(), e);
								return null;
							}).thenRun(() -> taskScheduler.schedule(this::reportProcessing, Instant.now().plusSeconds(5))));
		} finally {
			isActive.set(false);
		}
	}


	private void processReport(ReportRequestDTO reportRequestDTO, Long requestId) {
		try {
			ReportResponseDTO responseDTO = reportRequestService.updateProgress(requestId, 1);
			LoggedInUserDTO loggedInUserDTO = authenticationService.getLoggedInUserByUserId(responseDTO.getCreatedBy());
			if (loggedInUserDTO == null) {
				log.warn("No logged in user for user {}", responseDTO.getCreatedBy());
			}
			SecurityContextHolder.getContext().setAuthentication(loggedInUserDTO);
			ReportUrlDTO reportUrlDTO;
			if (reportRequestDTO.getReportId() != null && reportRequestDTO.getReportId() > 0) {
				reportUrlDTO = reportIdReportProcessProvider.process(reportRequestDTO.getReportId(), requestId);
			} else {
				reportUrlDTO = customReportProcessProvider.process(reportRequestDTO, requestId);
			}
			log.info("Request {} is processed", requestId);
			ReportRequest reportRequest = processDone(requestId, reportUrlDTO);
			DownloadUrlDTO url = generateUrl(reportRequest, s3UrlLifetime.toMillis());
			log.info("Sending report email for request {}", requestId);
			emailSender.sendHtmlizedMailByAWSMailSender(String.format(emailSubject, reportRequest.getReportName(), requestId), getEmailContent(url.getUrl()),
					fromEmail, reportRequest.getEmails());
			log.info("Request {} processing finished", requestId);
		} catch (Exception e) {
			processFailed(requestId, ExceptionUtils.getStackTrace(e));
			log.error("Report processing failed for request " + requestId, e);
		} finally {
			SecurityContextHolder.getContext().setAuthentication(null);
		}
	}

	private String getEmailContent(String url) {
		return "The export file you recently requested has finished generating. Please <a href='" + url + "'>click here</a> to download the report.<br>" +
				"<br>" +
				"Thank you!<br>";
	}

	@SneakyThrows
	private ReportRequest processDone(Long requestId, ReportUrlDTO reportUrlDTO) {
		ReportRequest reportRequest = reportRequestRepository.getById(requestId);
		reportRequest.setEndDate(Timestamp.from(Instant.now()));
		reportRequest.setProgressPercent(100);
		reportRequest.setStatus(ReportRequest.Status.DONE);
		reportRequest.setUrlData(mapper.writeValueAsString(reportUrlDTO));
		return reportRequestRepository.save(reportRequest);
	}

	private void processFailed(Long requestId, String failReason) {
		ReportRequest reportRequest = reportRequestRepository.getById(requestId);
		reportRequest.setEndDate(Timestamp.from(Instant.now()));
		reportRequest.setStatus(ReportRequest.Status.FAILED);
		reportRequest.setFailReason(failReason);
		reportRequestRepository.save(reportRequest);
	}

}
