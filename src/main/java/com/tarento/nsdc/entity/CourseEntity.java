package com.tarento.nsdc.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import lombok.*;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name = "courses")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CourseEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private String id;

    private String partner;
    private String courseName;

    @JsonProperty("isCoveredGoiIncentive")
    @Column(name = "is_covered_goi_incentive")
    private boolean isCoveredGoiIncentive;

    @JsonProperty("NASSCOMAccountLead")
    private String NASSCOMAccountLead;

    private String courseUrl;

    @Column(length = 1000)
    private String partnerDetails;

    private String partnerLogo;

    @Column(length = 1000)
    private String productDescription;

    @Column(length = 1000)
    private String learningOutcome;

    @Column(length = 1000)
    private String benefits;

    private String targetedAudience;

    @Column(length = 1000)
    private String curriculum;

    @Type(type = "jsonb")
    @Column(name = "tools", columnDefinition = "jsonb")
    private JsonNode tools;

    @Column(length = 1000)
    private String marketingUSP;

    @Type(type = "jsonb")
    @Column(name = "faqs", columnDefinition = "jsonb")
    private JsonNode faqs;

    @Column(name = "course_duration")
    private String courseDuration;

    private String skillType;

    @Column(name = "course_price")
    private double coursePrice;

    @Column(name = "promotion_price")
    private double promotionPrice;

    @Column(name = "promotion_start_date")
    private String promotionStartDate;

    @Column(name = "promotion_end_date")
    @Temporal(TemporalType.DATE)
    @JsonFormat(pattern = "dd/MM/yyyy")
    private Date promotionEndDate;

    private String pathway;
    private String deliveryMode;
    private String industryTech;
    private String courseCategory;
    private String jobRoles;

    @JsonProperty("isAligned")
    @Column(name = "is_aligned")
    private String isAligned;

    private String imageUrl;

    @Column(name = "is_fs_prime_badge")
    @JsonProperty("isFSPrimeBadge")
    private boolean isFSPrimeBadge;

    @Column(name = "is_course_completed")
    @JsonProperty("isCourseCompleted")
    private boolean isCourseCompleted;

    @Column(name = "is_incorporate")
    @JsonProperty("isIncorporate")
    private boolean isIncorporate;

    @Column(name = "placement_assistance")
    private boolean placementAssistance;

    private String sscNASSCOMAssessment;
}
