package ceo.peak.data.job;

import ceo.peak.data.entity.CompanyData;
import ceo.peak.data.repository.CompanyDataRepository;
import ceo.peak.data.util.AddressSimilarity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class SaraminBatch {

    private final JobRepository jobRepository;
    private final CompanyDataRepository companyDataRepository;

    @Bean
    public Job saraminJob(Step saraminStep) {
        return new JobBuilder("saraminJob", jobRepository)
                .start(saraminStep)
                .incrementer(new RunIdIncrementer()) // 파라미터 자동 증가
                .build();
    }

    @Bean
    public Step saraminStep(ItemReader<String> saraminCompanyCodeReader,
                            ItemProcessor<String, CompanyData> saraminCompanyDataProcessor,
                            ItemWriter<CompanyData> saraminWriter,
                            JobRepository jobRepository,
                            PlatformTransactionManager transactionManager) {
        return new StepBuilder("saraminStep", jobRepository)
                .<String, CompanyData>chunk(100, transactionManager)
                .reader(saraminCompanyCodeReader)
                .processor(saraminCompanyDataProcessor)
                .writer(saraminWriter)
                .faultTolerant()
                .retry(DataAccessException.class) // 데이터베이스 연결 실패, SQL 실행 오류, 제약 조건 위반 등 다양한 데이터 액세스 관련 예외
                .retry(TransientDataAccessException.class) // 데이터베이스 서버 과부하, 네트워크 문제, 일시적인 잠금 등의 예외
                .retryLimit(3) // 예외 발생 시 최대 3번 재시도
                .skip(IOException.class)
                .noSkip(IllegalArgumentException.class)
                .noSkip(NullPointerException.class)
                .skipLimit(100) // skip 가능한 exception 횟수 제한
                .build();
    }


    @Bean
    public ItemReader<String> saraminCompanyCodeReader() {
        return new ItemReader<String>() {
            private int currentPage = 1;
            private int maxPage = 100;
            private List<String> companyCodes;
            private int nextIndex = 0;
            private ExecutionContext executionContext;

            @BeforeStep
            public void beforeStep(StepExecution stepExecution) {
                executionContext = stepExecution.getExecutionContext();
                if (executionContext.containsKey("currentPage")) {
                    currentPage = executionContext.getInt("currentPage");
                }
                if (executionContext.containsKey("nextIndex")) {
                    nextIndex = executionContext.getInt("nextIndex");
                } else {
                    currentPage = 1;
                    nextIndex = 0;
                }
            }

            @Override
            public String read() throws Exception {
                if (companyCodes == null || nextIndex >= companyCodes.size()) {
                    if (currentPage > maxPage) {
                        log.info("No more company codes to read. Current page: {}, Max page: {}", currentPage, maxPage);
                        return null; // 더 이상 읽을 데이터가 없음
                    }
                    log.info("Fetching company codes from page: {}", currentPage);
                    companyCodes = performSaraminCompanyCodeCrawling(currentPage);
                    if (companyCodes.isEmpty()) {
                        log.warn("No company codes found on page: {}", currentPage);
                        return null;
                    }
                    nextIndex = 0;
                    currentPage++;
                }

                String companyCode = companyCodes.get(nextIndex++);
                executionContext.putInt("currentPage", currentPage);
                executionContext.putInt("nextIndex", nextIndex);
                return companyCode;
            }

            private List<String> performSaraminCompanyCodeCrawling(int page) throws IOException {
                List<String> companyCodes = new ArrayList<>();
                try {
                    String baseUrl = "https://www.saramin.co.kr/zf_user/salaries/total-salary/list?";
                    String params = "order=reg_dt&industry_cd=&company_cd=&rec_status=&group_cd=0&search_company_nm_org=&search_company_nm=&min_salary=1000&max_salary=10000&request_modify_company_nm=";
                    String url = baseUrl + "page=" + page + "&" + params;
                    Document doc = Jsoup.connect(url).get();
                    Elements companyLinks = doc.select("a.link_tit");
                    for (Element link : companyLinks) {
                        String href = link.attr("href");
                        String csn = extractCsn(href);
                        if (csn != null) {
                            companyCodes.add(csn);
                        }
                    }
                    log.info("Found {} company codes on page {}", companyCodes.size(), page);
                } catch (IOException e) {
                    log.error("Error while crawling company codes from page " + page, e);
                    throw e; // IOException 발생 시 예외를 던져 Spring Batch가 재시도 또는 스킵하도록 처리
                }
                return companyCodes;
            }

            private String extractCsn(String href) {
                try {
                    Pattern pattern = Pattern.compile("csn=([^&]+)");
                    Matcher matcher = pattern.matcher(href);
                    if (matcher.find()) {
                        return matcher.group(1);
                    } else {
                        return null;
                    }
                } catch (Exception e) {
                    log.error("Error while extracting CSN from href", e);
                    return null; // 예외 발생 시 null 반환
                }
            }
        };
    }

    @Bean
    public ItemProcessor<String, CompanyData> saraminCompanyDataProcessor() {
        return companyCode -> {
            String modifiedCompany = "-";
            String keyExecutive = "-";
            String industry = "-";
            String address = "-";
            String homepage = "-";
            String sales = "-";
            String logoUrl = "-";

            try {
                // 크롤링할 URL
                String url = "https://www.saramin.co.kr/zf_user/company-info/view?csn=" + companyCode;

                // HTML 문서 가져오기
                Document doc = Jsoup.connect(url)
                        .get();

                Element h1Element = doc.selectFirst("h1.tit_company");
                if (h1Element != null) {
                    String company = h1Element.attr("title");
                    // 정규표현식 패턴: 괄호 안의 문자와 "주식회사" 제거
                    String pattern = "\\([^)]*\\)\\s*|주식회사\\s*"; // 괄호 안의 문자와 그 뒤의 공백, "주식회사" 제거

                    // 문자열에서 패턴을 찾아 빈 문자열로 대체
                    modifiedCompany = company.replaceAll(pattern, "");
                    log.info("Processed company: " + modifiedCompany);
                } else {
                    log.warn("h1.tit_company element not found.");
                }

                Elements companyDetailsGroups = doc.select("div.company_details_group");
                for (Element group : companyDetailsGroups) {
                    Element dtElement = group.selectFirst("dt.tit");
                    if (dtElement != null) {
                        if (dtElement.text().equals("대표자명")) {
                            Element ddElement = group.selectFirst("dd.desc");
                            if (ddElement != null) {
                                keyExecutive = ddElement.text();
                            }
                        } else if (dtElement.text().equals("업종")) {
                            Element ddElement = group.selectFirst("dd.desc");
                            if (ddElement != null) {
                                industry = ddElement.text();
                            }
                        } else if (dtElement.text().equals("주소")) {
                            Element ddElement = group.selectFirst("dd.desc");
                            if (ddElement != null) {
                                Element pElement = ddElement.selectFirst("p.ellipsis");
                                if (pElement != null) {
                                    address = pElement.text();
                                }
                            }
                        } else if (dtElement.text().equals("홈페이지")) {
                            Element ddElement = group.selectFirst("dd.desc");
                            if (ddElement != null) {
                                homepage = ddElement.text();
                            }
                        }
                    }
                }

                Element companySummary = doc.selectFirst("ul.company_summary");
                if (companySummary != null) {
                    Elements companySummaryItems = companySummary.select("li.company_summary_item");
                    for (Element item : companySummaryItems) {
                        if (item.selectFirst("p.company_summary_desc") != null &&
                                item.selectFirst("p.company_summary_desc").text().equals("매출액")) {
                            Element strongElement = item.selectFirst("strong.company_summary_tit");
                            if (strongElement != null) {
                                sales = strongElement.text();
                            }
                        }
                    }
                }

                Element boxLogo = doc.selectFirst("div.box_logo");
                if (boxLogo != null) {
                    Element imgElement = boxLogo.selectFirst("img");
                    if (imgElement != null) {
                        logoUrl = imgElement.attr("src");
                    }
                }

            } catch (Exception e) {
                log.error("Error while processing company code: " + companyCode, e);
                return null; // 예외 발생 시 null 반환하여 skip
            }

            return CompanyData.of(modifiedCompany, keyExecutive, industry, address,
                    homepage, "-", "-", sales, "-", logoUrl);
        };
    }

    @Bean
    public ItemWriter<CompanyData> saraminWriter() {
        return items -> {
            List<CompanyData> newItems = new ArrayList<>();
            for (CompanyData item : items) {
                try {
                    List<CompanyData> existingDataByCompany = companyDataRepository.findByCompany(item.getCompany());

                    boolean updated = false;
                    for (CompanyData existingData : existingDataByCompany) {
                        double addressSimilarity = AddressSimilarity.similarity(existingData.getAddress(), item.getAddress());
                        if (addressSimilarity > 0.7) { // 주소 유사도가 0.7 이상일 경우 업데이트
                            existingData.update(
                                    item.getCompany(),
                                    item.getKeyExecutive(),
                                    item.getIndustry(),
                                    item.getAddress(),
                                    item.getHomepage(),
                                    item.getSales(),
                                    item.getLogoUrl()
                            );
                            companyDataRepository.save(existingData);
                            updated = true;
                            log.info("Updated existing company data: " + item.getCompany());
                            break;
                        }
                    }

                    if (!updated) {
                        // 일치하는 데이터가 없으면 새로운 데이터로 추가
                        newItems.add(item);
                    }
                } catch (DataAccessException e) {
                    log.error("Error while writing data to database for company: " + item.getCompany(), e);
                }
            }
            // 새로운 데이터만 일괄 저장
            try {
                if (!newItems.isEmpty()) {
                    companyDataRepository.saveAll(newItems);
                    log.info("Saved new company data. Count: " + newItems.size());
                }
            } catch (DataAccessException e) {
                log.error("Error while saving new company data to database.", e);
            }
        };
    }
}
