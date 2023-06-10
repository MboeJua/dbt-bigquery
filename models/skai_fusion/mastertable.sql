--CREATE OR REPLACE TABLE
--  bigquery-testing-368011.dbt_pmboejua.Master_Table AS
WITH
  mastertable1 AS (
  SELECT
   DISTINCT
    --items from Keywords by device
    FKBD.Date,
    FKBD.Ad_Group_Name,
    FKBD.Campaign_Name,
    FKBD.Channel,
    FKBD.Device,
    FKBD.Keyword,
    FKBD.Clicks,
    FKBD.Cost,
    FKBD.Rev_,
    FKBD.conversions,
  (FKBD.Rev_ - FKBD.Cost )Profit,
    FKBD.Profile_Name,
    FKBD.Account_Name,
    FKBD.Ad_Group_ID,
    FKBD.AdGroup_ID_in_Kenshoo,
    FKBD.AdGroup_Status,
    FKBD.Bid,
    FKBD.Campaign_ID,
    FKBD.Campaign_Status,
    FKBD.Channel_Account_ID,
    FKBD.Channel_Account_Publisher_ID,
    FKBD.Channel_Ad_Group_ID,
    FKBD.Channel_Campaign_ID,
    FKBD.Channel_Keyword_ID,
    FKBD.Custom_Parameter_1,
    FKBD.Custom_Parameter_2,
    FKBD.Custom_Parameter_3,
    FKBD.Keyword_Creation_Date,
    FKBD.Keyword_ID,
    FKBD.Land_URL Land_URL_Keywords,
    FKBD.Last_Modified,
    FKBD.Match_Type,
    FKBD.Mobile_Bid_Adjustment,
    FKBD.Mobile_URL,
    FKBD.Network,
    FKBD.Profile_Currency,
    FKBD.Profile_ID,
    FKBD.Quality_Score,
    FKBD.Status,
    FKBD.Tracking_Template,
    FKBD.Upgr__URL_Ind_,
    FKBD.Imp__Share,
    FKBD.Impr___Abs_Top__,
    FKBD.Impr___Top__,
    FKBD.Impressions,
    FKBD.Lost_IS_rank,
    FKBD.Potential_Impressions,
    FKBD.Search__Abs_Top__IS_,
    FKBD.Search__Top__IS_,
    FKBD.ga4_form_lead,
    FKBD.ga4_call_lead,
    FKBD.Office_Designation,
    FKBD.Owner,
    FKBD.Experiment,

    --- items from Ads by Device
     FABD.Date Date2,
  FABD.Currency Currency2,
  FABD.Ad_Group_Name Ad_Group_Name2,
  FABD.Campaign_Name Campaign_Name2,
  FABD.Channel Channel2,
  FABD.Device Device2,
  FABD.Headline Headline2,
  FABD.Profile_Name Profile_Name2,
  FABD.Account_Name Account_Name2,
  FABD.Ad_Group_ID Ad_Group_ID2,
  FABD.Ad_Name Ad_Name2,
  FABD.Ad_Type Ad_Type2,
  FABD.AdGroup_ID_in_Kenshoo AdGroup_ID_in_Kenshoo2,
  FABD.AdGroup_Status AdGroup_Status2,
  FABD.Campaign_ID Campaign_ID2,
  FABD.Campaign_Status Campaign_Status2,
  FABD.Channel_Account_ID Channel_Account_ID2,
  FABD.Channel_Account_Publisher_ID Channel_Account_Publisher_ID2,
  FABD.Channel_Ad_Group_ID Channel_Ad_Group_ID2,
  FABD.Channel_Campaign_ID Channel_Campaign_ID2,
  FABD.Profile_ID Profile_ID2,
  FABD.Promotion Promotion2,
  FABD.Quality_Score Quality_Score2,
  FABD.Status Status2,
  FABD.Channel_Ad_ID Channel_Ad_ID2,
  FABD.Avg_Pos_ Avg_Pos_2,
    FABD.Network Network2,
    FABD.Tracking_ID, 
    FABD.Headline,
    FABD.Display_URL,
    FABD.Headline_2,
    FABD.Headline_3,
    FABD.Image_Name,
    FABD.Line_1,
    FABD.Line_2,
    FABD.Path_1,
    FABD.Path_2,
    FABD.Promotion,
    FABD.Channel_Ad_id,
    FABD.land_url Land_url_ads,
    FABD.rev_ rev_ads,
    FABD.conversions Conversion_ads,
    FABD.Quality_Score Quality_Score_ads,
    FABD.Avg_Pos_,
    FABD.Impr___Abs_Top__ Impr___Abs_Top__ads,
    FABD.Impr___Top__ Impr___Top__ads,
    FABD.Impressions Impressions_ads,
    FABD.Clicks Clicks_ads,
    FABD.Cost Cost_ads,
    FABD.ga4_form_lead ga4_form_leads_ads,
    (FABD.Rev_ - FABD.Cost)  Profit_ads,
    FABD.ads_key

  
  FROM
    `bigquery-testing-368011.dbt_pmboejua.Fusion_Keywords` FKBD
   JOIN
    (select *, ("ads_key") ads_key
    from `bigquery-testing-368011.dbt_pmboejua.Fusion_Ads`)FABD
  ON
    FKBD.Date = FABD.Date
    AND FKBD.Campaign_Name = FABD.Campaign_Name
    AND FKBD.Account_Name = FABD.Account_Name
    AND FKBD.Campaign_ID= FABD.Campaign_ID
    AND FKBD.Channel_Account_ID = FABD.Channel_Account_ID
    AND FKBD.Channel = FABD.Channel
    AND FKBD.Device = FABD.Device
    AND FKBD.Ad_Group_Name = FABD.Ad_Group_Name
    AND FKBD.Channel_Ad_Group_ID = FABD.Channel_Ad_Group_ID
    AND FKBD.AdGroup_ID_in_Kenshoo = FABD.AdGroup_ID_in_Kenshoo
    AND FKBD.Channel_Campaign_ID = FABD.Channel_Campaign_ID
  


   ),
  mastertable2 as ( 
SELECT
  *,row_number() OVER(PARTITION BY 
 tracking_id, date2, profile_name2,device2,
 Channel_Ad_Group_ID2, Channel_Campaign_ID2,Ad_Group_ID2,
AdGroup_ID_in_Kenshoo2,channel2





--Tracking_ID
 --order by Tracking_ID_ads
 ) as rank_ads,



 
 row_number() OVER(PARTITION BY Keyword,Date,Campaign_Name, Ad_Group_Name, 
Channel, Device,  Profile_Name,
Account_Name, Ad_Group_ID,
AdGroup_ID_in_Kenshoo, AdGroup_Status, 
Campaign_ID, Campaign_Status,
Channel_Account_ID, Channel_Account_Publisher_ID,
Channel_Ad_Group_ID, Channel_Campaign_ID, Channel_Keyword_ID,
Custom_Parameter_1, Custom_Parameter_2, Custom_Parameter_3,
Keyword_Creation_Date, Keyword_ID, Land_URL_Keywords,
Last_Modified, Match_Type,
Profile_Currency, Profile_ID, Quality_Score_ads, Status

) as rank_keywords,
FROM
  mastertable1 )
  
  select Ad_Group_Name, Date, Campaign_Name, Channel, Device, Keyword, Clicks, Cost, Rev_, conversions, Profit, Profile_Name, Account_Name, Ad_Group_ID, AdGroup_ID_in_Kenshoo, AdGroup_Status, Bid, Campaign_ID, Campaign_Status, Channel_Account_ID, Channel_Account_Publisher_ID, Channel_Ad_Group_ID, Channel_Campaign_ID, Channel_Keyword_ID, Custom_Parameter_1, Custom_Parameter_2, Custom_Parameter_3, Keyword_Creation_Date, Keyword_ID, Land_URL_Keywords, Last_Modified, Match_Type, Mobile_Bid_Adjustment, Mobile_URL, Network, Profile_Currency, Profile_ID, Quality_Score, Status, Tracking_Template, Upgr__URL_Ind_, Imp__Share, Impr___Abs_Top__, Impr___Top__, Impressions, Lost_IS_rank, Potential_Impressions, Search__Abs_Top__IS_, Search__Top__IS_, ga4_form_lead, ga4_call_lead, Office_Designation, Owner, Experiment, Tracking_ID, Headline, Display_URL, Headline_2, Headline_3, Image_Name, Line_1, Line_2, Path_1, Path_2, Promotion, Channel_Ad_id, Land_url_ads, rev_ads, Conversion_ads, Quality_Score_ads, Avg_Pos_, Impr___Abs_Top__ads, Impr___Top__ads, Impressions_ads, Clicks_ads, Cost_ads, ga4_form_leads_ads,
   Profit_ads, ads_key, rank_ads, rank_keywords
  from mastertable2
  union all
  select (null) Ad_Group_Name, Date, Campaign_Name, Channel, Device, (null) Keyword, Clicks, Cost, Rev_, conversions, ( Rev_- Cost)Profit, Profile_Name, Account_Name, (null) Ad_Group_ID, (null) AdGroup_ID_in_Kenshoo,(null) AdGroup_Status,(null) Bid, Campaign_ID,(null) Campaign_Status, Channel_Account_ID, Channel_Account_Publisher_ID,(null) Channel_Ad_Group_ID, Channel_Campaign_ID,(null) Channel_Keyword_ID,(null) Custom_Parameter_1,(null) Custom_Parameter_2,(null) Custom_Parameter_3,(null) Keyword_Creation_Date,(null) Keyword_ID,(null) Land_URL_Keywords, Last_Modified,(null) Match_Type, Mobile_Bid_Adjustment,(null) Mobile_URL, Network, Profile_Currency, Profile_ID,(null) Quality_Score, Status,(null) Tracking_Template,(null) Upgr__URL_Ind_,(null) Imp__Share, Impr___Abs_Top__, Impr___Top__, Impressions,(null) Lost_IS_rank,(null) Potential_Impressions,(null) Search__Abs_Top__IS_,(null) Search__Top__IS_, ga4_form_lead, ga4_call_lead,(null) Office_Designation,(null) Owner,(null) Experiment,(null) Tracking_ID,(null) Headline,(null) Display_URL,(null) Headline_2,(null) Headline_3,(null) Image_Name,(null) Line_1,(null) Line_2,(null) Path_1,(null) Path_2,(null) Promotion,(null) Channel_Ad_id,(null) Land_url_ads,(rev_) rev_ads,(conversions) Conversion_ads,(null) Quality_Score_ads,(null) Avg_Pos_,(Impr___Abs_Top__) Impr___Abs_Top__ads,(Impr___Top__) Impr___Top__ads,(Impressions) Impressions_ads,(Clicks) Clicks_ads,(cost) Cost_ads,(ga4_form_lead) ga4_form_leads_ads,--
  (Rev_ - Cost)Profit_ads,("ads_key") ads_key,(1) rank_ads,(1) rank_keywords
  from `bigquery-testing-368011.dbt_pmboejua.Fusion_Campaigns`
  