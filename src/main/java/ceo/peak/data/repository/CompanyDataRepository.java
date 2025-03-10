package ceo.peak.data.repository;

import ceo.peak.data.entity.CompanyData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface CompanyDataRepository extends JpaRepository<CompanyData, Long> {

    @Query("SELECT c FROM CompanyData c WHERE c.company = :company")
    List<CompanyData> findByCompany(@Param("company") String company);
}
