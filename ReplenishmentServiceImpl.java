package com.eriabank.houseinbox.datacenter.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.ClientAnchor.AnchorType;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.eriabank.houseinbox.authorize.api.IUserRoleApi;
import com.eriabank.houseinbox.base.api.IFourOrganizationApi;
import com.eriabank.houseinbox.base.api.IMerchandiseApi;
import com.eriabank.houseinbox.base.api.IMerchandiseImageApi;
import com.eriabank.houseinbox.base.api.IOrganizationApi;
import com.eriabank.houseinbox.base.api.IOrganizationRelativeMerchandiseApi;
import com.eriabank.houseinbox.base.api.IPropertiesApi;
import com.eriabank.houseinbox.base.api.IThirdAccountApi;
import com.eriabank.houseinbox.base.api.IUserApi;
import com.eriabank.houseinbox.base.api.dto.FinancialCompanyDto;
import com.eriabank.houseinbox.base.api.dto.FourOrganizationDto;
import com.eriabank.houseinbox.base.api.dto.FourOrganizationQueryDto;
import com.eriabank.houseinbox.base.api.dto.MerchandiseDto;
import com.eriabank.houseinbox.base.api.dto.MerchandiseImageDto;
import com.eriabank.houseinbox.base.api.dto.OrganizationDto;
import com.eriabank.houseinbox.base.api.dto.OrganizationQueryDto;
import com.eriabank.houseinbox.base.api.dto.OrganizationRelativeMerchandiseDto;
import com.eriabank.houseinbox.base.api.dto.PropertiesDto;
import com.eriabank.houseinbox.base.api.dto.ThirdAccountDto;
import com.eriabank.houseinbox.base.api.dto.ThirdAccountQueryDto;
import com.eriabank.houseinbox.base.api.dto.UserDto;
import com.eriabank.houseinbox.base.api.dto.UserQueryDto;
import com.eriabank.houseinbox.common.domain.CacheDomain;
import com.eriabank.houseinbox.common.domain.CommonEnumNumber;
import com.eriabank.houseinbox.common.domain.Context;
import com.eriabank.houseinbox.common.domain.FourUnitMerchantRepl;
import com.eriabank.houseinbox.common.domain.KaReplChartQuery;
import com.eriabank.houseinbox.common.domain.PageDomain;
import com.eriabank.houseinbox.common.result.BaseResult;
import com.eriabank.houseinbox.common.util.BaseDataUtil;
import com.eriabank.houseinbox.common.util.CommonNumberMessage;
import com.eriabank.houseinbox.common.util.DateUtil;
import com.eriabank.houseinbox.common.util.JsonUtils;
import com.eriabank.houseinbox.common.util.ListUtil;
import com.eriabank.houseinbox.common.util.NumberUtil;
import com.eriabank.houseinbox.common.util.PrintUtils;
import com.eriabank.houseinbox.common.util.StringUtil;
import com.eriabank.houseinbox.datacenter.domain.BasicPlatAccUnitCompany;
import com.eriabank.houseinbox.datacenter.domain.EcommerceMonitor;
import com.eriabank.houseinbox.datacenter.domain.EcommerceMonitorSimplified;
import com.eriabank.houseinbox.datacenter.domain.FinancePermissionResult;
import com.eriabank.houseinbox.datacenter.domain.FourUnitReplXW;
import com.eriabank.houseinbox.datacenter.domain.FourUnitSales;
import com.eriabank.houseinbox.datacenter.domain.KaFinanceRankingQuery;
import com.eriabank.houseinbox.datacenter.domain.KaFinanceRankingResult;
import com.eriabank.houseinbox.datacenter.domain.KaPlatformStockForHIB;
import com.eriabank.houseinbox.datacenter.domain.KaReplSiteSaledetail;
import com.eriabank.houseinbox.datacenter.domain.KaSkuSeriesMapping;
import com.eriabank.houseinbox.datacenter.domain.KaStockForHIBResult;
import com.eriabank.houseinbox.datacenter.domain.MerchantReplQuery;
import com.eriabank.houseinbox.datacenter.domain.OnlineInventory;
import com.eriabank.houseinbox.datacenter.domain.OnlineInventoryQuery;
import com.eriabank.houseinbox.datacenter.service.IReplenishmentService;
import com.eriabank.houseinbox.datacenter.util.ExcelUtil;
import com.eriabank.houseinbox.datacenter.util.HttpUtil;
import com.eriabank.houseinbox.erp.api.IErpCloudMaterialNumApi;
import com.eriabank.houseinbox.erp.api.dto.ErpCloudMaterialAllDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

@Service("replenishmentService")
public class ReplenishmentServiceImpl implements IReplenishmentService {
    private Logger logger = LoggerFactory.getLogger(ReplenishmentServiceImpl.class);

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public ReplenishmentServiceImpl(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }
    
    @Autowired
	private IPropertiesApi propertiesApi;
    @Autowired
    private IUserApi userApi;
    @Autowired
    private IUserRoleApi userRoleApi;
    
    @Autowired
	private IThirdAccountApi thirdAccountApi;
    @Autowired
    private IOrganizationApi organizationApi;
    @Autowired
    private IFourOrganizationApi fourOrganizationApi;
    
    @Autowired
	private IMerchandiseApi merchandiseApi;
    @Autowired
    private IMerchandiseImageApi merchandiseImageApi;
    
    @Autowired
    private IOrganizationRelativeMerchandiseApi organizationRelativeMerchandiseApi;
    
    @Autowired
    private IErpCloudMaterialNumApi erpCloudMaterialNumApi;
    
    

    /**
     * 页面初始化
     * @return
     */
    @Override
    public String pageInit() throws JsonProcessingException{
        String remoteUrl = "http://139.159.217.92:8000/datacenter/settlement/merchantRepl/pageInit";
        //String localUrl = "http://localhost:9080/merchantRepl/pageInit" ;
        String url = remoteUrl;
        Map<String, Object> requestParam = new HashMap<String, Object>();
        String requestBody = objectMapper.writeValueAsString(requestParam);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> requestEntity = new HttpEntity<String>(requestBody, headers);
        ResponseEntity<String> resEntity = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);

        return resEntity.getBody();
    }

    @Override
    public String selectByPage(MerchantReplQuery aquery, PageDomain pageDomain) throws JsonProcessingException {
        //区分传统与电商
        String urlHandle = "selectEcommerceData";
        if(aquery.getIsTradition() != null && aquery.getIsTradition()){
            urlHandle = "selectTraditionalData";
        }
        String remoteUrl = "http://139.159.217.92:8000/datacenter/settlement/merchantRepl/" + urlHandle;
        //String localUrl = "http://localhost:9080/merchantRepl/" + urlHandle;
        String url = remoteUrl;
        Map<String, Object> requestParam = new HashMap<String, Object>();
        requestParam.put("aquery", aquery);
        requestParam.put("pageDomain", pageDomain);
        String requestBody =  objectMapper.writeValueAsString(requestParam);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> requestEntity = new HttpEntity<String>(requestBody, headers);
        ResponseEntity<String> resEntity = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);

        return resEntity.getBody();
    }

	@Override
	public void downloadTH(Context context, HttpServletResponse response) {
		// TODO 20221111 太华补货表库存excel导出
		
		ObjectMapper mapper = new ObjectMapper();
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.TH_DOWN_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("太华补货表库存接口地址 url : {}", url);
		//参数
		KaReplChartQuery replQuery = new KaReplChartQuery();
		
		List<FourUnitMerchantRepl> thStockList = new ArrayList<>();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				thStockList = mapper.readValue(responseMsg, new TypeReference<List<FourUnitMerchantRepl>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "太华补货表库存接口失败："+e); 
		}
		logger.info("thStockList size : {}", thStockList.size());
		
		//20221205 过滤，汇总
		thStockList = getSumReplData(thStockList);
		
		List<FourUnitMerchantRepl> thUSStockList = new ArrayList<>();	
		List<FourUnitMerchantRepl> thCAStockList = new ArrayList<>();		
		for (FourUnitMerchantRepl replDetail : thStockList) {
			String countryCode = replDetail.getCountry() == null ? "" : replDetail.getCountry();
			if(countryCode.equalsIgnoreCase("US")){
				thUSStockList.add(replDetail);
			}
			else if(countryCode.equalsIgnoreCase("CA")){
				thCAStockList.add(replDetail);
			}
		}
		
		
		OutputStream os = null;
		
		//String pathName = "线上库存表导出数据.xls";
		String pathName = "39F Inventory.xls";//6、文件名字不要中文，改成39F Inventory
		SXSSFWorkbook workbook = new SXSSFWorkbook(500);
		
		buildUSExcel(workbook, thUSStockList);
		workbook = buildCAExcel(workbook, thCAStockList);
		//workbook = buildWarehouseAdressExcel(workbook);//2. warehouse address这个sheet删除
		
		
		try {
            response.reset();
            response.setContentType("application/vnd.ms-excel;charset=UTF-8");
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Disposition", "inline; filename=" + pathName);
            response.setHeader("fname", pathName);
            response.setHeader("flag", "1");
            os = response.getOutputStream();
            workbook.write(os);
            os.flush();
        } catch (Exception e) {
            logger.info("exportExcel", e);
            response.setHeader("flag", "0");
            response.setHeader("msg", e.getMessage());
        } finally {
            try {
                if (os != null)
                    os.close();
                workbook.close();
            } catch (IOException e) {
                logger.info("exportExcel close os or wb", e);
            }
        }
	}
	
	
	private List<FourUnitMerchantRepl> getSumReplData(List<FourUnitMerchantRepl> fourUnitReplList) {
		// TODO 过滤，汇总数据
		//过滤，汇总
        Map<String, FourUnitMerchantRepl> replBySumKeyMap = new HashMap<>();
        for (FourUnitMerchantRepl replDetail : fourUnitReplList) {
			if(replDetail.getInventorySaleableOs() == null) {
				continue;
			}
			
			String category = replDetail.getCategory() == null ? "" : replDetail.getCategory();
			if(category.equalsIgnoreCase("Spare Parts")){
				continue;
			}
			
			/** 汇总 */
			//Rancho(仓1)
			Integer rancho = replDetail.getWarehouseOne() == null ? 0 : replDetail.getWarehouseOne();
			//Ontario(仓2)
			Integer ontario = replDetail.getWarehouseTwo() == null ? 0 : replDetail.getWarehouseTwo();
			//Atlanta(仓3)
			Integer atlanta = replDetail.getWarehouseThree() == null ? 0 : replDetail.getWarehouseThree();
			//Houston(优衣库(Houston))
			Integer houston = replDetail.getHouston() == null ? 0 : replDetail.getHouston();
			//US CCP(齐物(US CCP))
			Integer usCcp = replDetail.getUsCcp() == null ? 0 : replDetail.getUsCcp();
			//US LSQ(天道(US LSQ))
			Integer usLsq = replDetail.getUsLsq() == null ? 0 : replDetail.getUsLsq();
			//US GSP(人间(US GSP))
			Integer usGsp = replDetail.getUsGsp() == null ? 0 : replDetail.getUsGsp();
			//US CHA(月令(US CHA))
			Integer usCha = replDetail.getUsCha() == null ? 0 : replDetail.getUsCha();
			//US ATL(天下(US ATL))
			Integer usAtl = replDetail.getUsAtl() == null ? 0 : replDetail.getUsAtl();
			//US ONO(北游(US ONO))
			Integer usOno = replDetail.getUsOno() == null ? 0 : replDetail.getUsOno();
			//US SNA(说剑(US SNA))
			Integer usSna = replDetail.getUsSna() == null ? 0 : replDetail.getUsSna();
			//US BUR(达生(US BUR))
			Integer usBur = replDetail.getUsBur() == null ? 0 : replDetail.getUsBur();
			//US PDK(桑楚(US PDK))
			Integer usPdk = replDetail.getUsPdk() == null ? 0 : replDetail.getUsPdk();
			//US PHL(天运(US PHL))
			Integer usPhl = replDetail.getUsPhl() == null ? 0 : replDetail.getUsPhl();

			int stockAllUS = rancho + ontario + atlanta + houston + usCcp + usLsq + usGsp + usCha + usAtl + usOno 
								+ usSna + usBur + usPdk + usPhl;

			//Toronto(仓1)
			//Integer toronto = replDetail.getWarehouseOne() == null ? 0 : replDetail.getWarehouseOne();
			Integer toronto = rancho;
			//RICHMOND HILL(吉野家(RICHMOND HILL))
			Integer richmondHill = replDetail.getRichmondHill() == null ? 0 : replDetail.getRichmondHill();
			//YUL(问辩(CA YUL))
			Integer caYul = replDetail.getCaYul() == null ? 0 : replDetail.getCaYul();
			//YTZ(逍遥(CA YTZ))
			Integer caYtz = replDetail.getCaYtz() == null ? 0 : replDetail.getCaYtz();
			//Delta(三角洲(Delta))
			Integer delta = replDetail.getDelta() == null ? 0 : replDetail.getDelta();
			//YMX(说疑(CA YMX))
			Integer caYmx = replDetail.getCaYmx() == null ? 0 : replDetail.getCaYmx();

			int stockAllCA = toronto + richmondHill + caYul + caYtz + delta + caYmx;
			
			BigDecimal stockAll = new BigDecimal(stockAllUS).add(new BigDecimal(stockAllCA));
			if(stockAll.compareTo(BigDecimal.ZERO) ==0) {
				continue;
			}
			
			//国家 + SKU
			String sumKey = replDetail.getCountry() + "," + replDetail.getSku();
			if(replBySumKeyMap.containsKey(sumKey)) {
				FourUnitMerchantRepl merchantRepl = replBySumKeyMap.get(sumKey);
				//US
				merchantRepl.setWarehouseOne(rancho + (merchantRepl.getWarehouseOne() == null ? 0 : merchantRepl.getWarehouseOne()));
				merchantRepl.setWarehouseTwo(ontario + (merchantRepl.getWarehouseTwo() == null ? 0 : merchantRepl.getWarehouseTwo()));
				merchantRepl.setWarehouseThree(atlanta + (merchantRepl.getWarehouseThree() == null ? 0 : merchantRepl.getWarehouseThree()));
				merchantRepl.setHouston(houston + (merchantRepl.getHouston() == null ? 0 : merchantRepl.getHouston()));
				merchantRepl.setUsCcp(usCcp + (merchantRepl.getUsCcp() == null ? 0 : merchantRepl.getUsCcp()));
				merchantRepl.setUsLsq(usLsq + (merchantRepl.getUsLsq() == null ? 0 : merchantRepl.getUsLsq()));
				merchantRepl.setUsGsp(usGsp + (merchantRepl.getUsGsp() == null ? 0 : merchantRepl.getUsGsp()));
				merchantRepl.setUsCha(usCha + (merchantRepl.getUsCha() == null ? 0 : merchantRepl.getUsCha()));
				merchantRepl.setUsAtl(usAtl + (merchantRepl.getUsAtl() == null ? 0 : merchantRepl.getUsAtl()));
				merchantRepl.setUsOno(usOno + (merchantRepl.getUsOno() == null ? 0 : merchantRepl.getUsOno()));
				merchantRepl.setUsSna(usSna + (merchantRepl.getUsSna() == null ? 0 : merchantRepl.getUsSna()));
				merchantRepl.setUsBur(usBur + (merchantRepl.getUsBur() == null ? 0 : merchantRepl.getUsBur()));
				merchantRepl.setUsPdk(usPdk + (merchantRepl.getUsPdk() == null ? 0 : merchantRepl.getUsPdk()));
				merchantRepl.setUsPhl(usPhl + (merchantRepl.getUsPhl() == null ? 0 : merchantRepl.getUsPhl()));
				//CA
				merchantRepl.setRichmondHill(richmondHill + (merchantRepl.getRichmondHill() == null ? 0 : merchantRepl.getRichmondHill()));
				merchantRepl.setCaYul(caYul + (merchantRepl.getCaYul() == null ? 0 : merchantRepl.getCaYul()));
				merchantRepl.setCaYtz(caYtz + (merchantRepl.getCaYtz() == null ? 0 : merchantRepl.getCaYtz()));
				merchantRepl.setDelta(delta + (merchantRepl.getDelta() == null ? 0 : merchantRepl.getDelta()));
				merchantRepl.setCaYmx(caYmx + (merchantRepl.getCaYmx() == null ? 0 : merchantRepl.getCaYmx()));
				
			} else {
				replBySumKeyMap.put(sumKey, replDetail);
			}
			
		}
        
        List<FourUnitMerchantRepl> fourNewReplList = new ArrayList<>();//返回结果集
        if(replBySumKeyMap.size() > 0) {
        	for (Entry<String, FourUnitMerchantRepl> replEntry : replBySumKeyMap.entrySet()) {
        		fourNewReplList.add(replEntry.getValue());
			}
        }
        
        return fourNewReplList;
	}

	private SXSSFWorkbook buildUSExcel(SXSSFWorkbook workbook, List<FourUnitMerchantRepl> thUSStockList){
		//3. 第二列的名字从Country改成Warehouse country
		String[] titles = {"No.", "Warehouse country", "Product name", "Category", "Total Available qty", "Rancho", "Ontario", "Atlanta", "Houston",
				"US CCP", "US LSQ", "US GSP", "US CHA", "US ATL", "US ONO", "US SNA", "US BUR",
				"US PDK", "US PHL", "SKU No."};
		SXSSFSheet sheet = null;
		SXSSFRow rowIndex = null;
		sheet = workbook.createSheet("US");
		rowIndex = sheet.createRow(0);
		for (int i = 0; i < titles.length; i++) {
			rowIndex.createCell(i).setCellValue(titles[i]);
		}

		int index = 0;
		int count = 1;
		if (thUSStockList != null && thUSStockList.size() > 0) {
			for (FourUnitMerchantRepl replDetail : thUSStockList) {
				index = 0;
				rowIndex = sheet.createRow(count);
				
				Integer stockAll = 0;
				//Rancho(仓1)
				Integer rancho = replDetail.getWarehouseOne() == null ? 0 : replDetail.getWarehouseOne();
				//Ontario(仓2)
				Integer ontario = replDetail.getWarehouseTwo() == null ? 0 : replDetail.getWarehouseTwo();
				//Atlanta(仓3)
				Integer atlanta = replDetail.getWarehouseThree() == null ? 0 : replDetail.getWarehouseThree();
				//Houston(优衣库(Houston))
				Integer houston = replDetail.getHouston() == null ? 0 : replDetail.getHouston();
				//US CCP(齐物(US CCP))
				Integer usCcp = replDetail.getUsCcp() == null ? 0 : replDetail.getUsCcp();
				//US LSQ(天道(US LSQ))
				Integer usLsq = replDetail.getUsLsq() == null ? 0 : replDetail.getUsLsq();
				//US GSP(人间(US GSP))
				Integer usGsp = replDetail.getUsGsp() == null ? 0 : replDetail.getUsGsp();
				//US CHA(月令(US CHA))
				Integer usCha = replDetail.getUsCha() == null ? 0 : replDetail.getUsCha();
				//US ATL(天下(US ATL))
				Integer usAtl = replDetail.getUsAtl() == null ? 0 : replDetail.getUsAtl();
				//US ONO(北游(US ONO))
				Integer usOno = replDetail.getUsOno() == null ? 0 : replDetail.getUsOno();
				//US SNA(说剑(US SNA))
				Integer usSna = replDetail.getUsSna() == null ? 0 : replDetail.getUsSna();
				//US BUR(达生(US BUR))
				Integer usBur = replDetail.getUsBur() == null ? 0 : replDetail.getUsBur();
				//US PDK(桑楚(US PDK))
				Integer usPdk = replDetail.getUsPdk() == null ? 0 : replDetail.getUsPdk();
				//US PHL(天运(US PHL))
				Integer usPhl = replDetail.getUsPhl() == null ? 0 : replDetail.getUsPhl();
				
				stockAll = rancho + ontario + atlanta + houston + usCcp + usLsq + usGsp + usCha + usAtl + usOno 
					+ usSna + usBur + usPdk + usPhl;

				
				//No.
				rowIndex.createCell(index++).setCellValue(count);
				//Country
				rowIndex.createCell(index++).setCellValue(replDetail.getCountry());
				//Product name
				rowIndex.createCell(index++).setCellValue(replDetail.getMeterial());
				//Category
				rowIndex.createCell(index++).setCellValue(replDetail.getCategory());
				//Total Available qty
				rowIndex.createCell(index++).setCellValue(stockAll);
				
				//Rancho
				if(rancho > 0) {
					rowIndex.createCell(index++).setCellValue(rancho);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Ontario
				if(ontario > 0) {
					rowIndex.createCell(index++).setCellValue(ontario);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Atlanta
				if(atlanta > 0) {
					rowIndex.createCell(index++).setCellValue(atlanta);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Houston
				if(houston > 0) {
					rowIndex.createCell(index++).setCellValue(houston);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US CCP
				if(usCcp > 0) {
					rowIndex.createCell(index++).setCellValue(usCcp);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US LSQ
				if(usLsq > 0) {
					rowIndex.createCell(index++).setCellValue(usLsq);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US GSP
				if(usGsp > 0) {
					rowIndex.createCell(index++).setCellValue(usGsp);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US CHA
				if(usCha > 0) {
					rowIndex.createCell(index++).setCellValue(usCha);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US ATL
				if(usAtl > 0) {
					rowIndex.createCell(index++).setCellValue(usAtl);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US ONO
				if(usOno > 0) {
					rowIndex.createCell(index++).setCellValue(usOno);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US SNA
				if(usSna > 0) {
					rowIndex.createCell(index++).setCellValue(usSna);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US BUR
				if(usBur > 0) {
					rowIndex.createCell(index++).setCellValue(usBur);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US PDK
				if(usPdk > 0) {
					rowIndex.createCell(index++).setCellValue(usPdk);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//US PHL
				if(usPhl > 0) {
					rowIndex.createCell(index++).setCellValue(usPhl);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//SKU No.
				rowIndex.createCell(index++).setCellValue(replDetail.getSku());
				
				count++;
			}
		}
		return workbook;
	}
	
	
	private SXSSFWorkbook buildCAExcel(SXSSFWorkbook workbook, List<FourUnitMerchantRepl> thCAStockList){
		////3. 第二列的名字从Country改成Warehouse country
		String[] titles = {"No.", "Warehouse country", "Product name", "Category", "Total Available qty", "Toronto", "RICHMOND HILL", "YUL", "YTZ",
				"Delta", "YMX", "SKU No."};
		SXSSFSheet sheet = null;
		SXSSFRow rowIndex = null;
		sheet = workbook.createSheet("CAN");//20221205 1. 下载文件中，加仓库存sheet名"CA"改成“CAN"
		rowIndex = sheet.createRow(0);
		for (int i = 0; i < titles.length; i++) {
			rowIndex.createCell(i).setCellValue(titles[i]);
		}

		int index = 0;
		int count = 1;
		if (thCAStockList != null && thCAStockList.size() > 0) {
			for (FourUnitMerchantRepl replDetail : thCAStockList) {
				index = 0;
				rowIndex = sheet.createRow(count);
				
				Integer stockAll = 0;
				//Toronto(仓1)
				Integer toronto = replDetail.getWarehouseOne() == null ? 0 : replDetail.getWarehouseOne();
				//RICHMOND HILL(吉野家(RICHMOND HILL))
				Integer richmondHill = replDetail.getRichmondHill() == null ? 0 : replDetail.getRichmondHill();
				//YUL(问辩(CA YUL))
				Integer caYul = replDetail.getCaYul() == null ? 0 : replDetail.getCaYul();
				//YTZ(逍遥(CA YTZ))
				Integer caYtz = replDetail.getCaYtz() == null ? 0 : replDetail.getCaYtz();
				//Delta(三角洲(Delta))
				Integer delta = replDetail.getDelta() == null ? 0 : replDetail.getDelta();
				//YMX(说疑(CA YMX))
				Integer caYmx = replDetail.getCaYmx() == null ? 0 : replDetail.getCaYmx();
				
				stockAll = toronto + richmondHill + caYul + caYtz + delta + caYmx;

				
				//No.
				rowIndex.createCell(index++).setCellValue(count);
				//Country
				rowIndex.createCell(index++).setCellValue(replDetail.getCountry());
				//Product name
				rowIndex.createCell(index++).setCellValue(replDetail.getMeterial());
				//Category
				rowIndex.createCell(index++).setCellValue(replDetail.getCategory());
				//Total Available qty
				rowIndex.createCell(index++).setCellValue(stockAll);
				
				//Toronto(仓1)
				if(toronto > 0) {
					rowIndex.createCell(index++).setCellValue(toronto);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//RICHMOND HILL(吉野家(RICHMOND HILL))
				if(richmondHill > 0) {
					rowIndex.createCell(index++).setCellValue(richmondHill);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//YUL(问辩(CA YUL))
				if(caYul > 0) {
					rowIndex.createCell(index++).setCellValue(caYul);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//YTZ(逍遥(CA YTZ))
				if(caYtz > 0) {
					rowIndex.createCell(index++).setCellValue(caYtz);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Delta(三角洲(Delta))
				if(delta > 0) {
					rowIndex.createCell(index++).setCellValue(delta);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//YMX(说疑(CA YMX))
				if(caYmx > 0) {
					rowIndex.createCell(index++).setCellValue(caYmx);
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//SKU No.
				rowIndex.createCell(index++).setCellValue(replDetail.getSku());
				
				count++;
			}
		}
		return workbook;
	}
	
	private SXSSFWorkbook buildWarehouseAdressExcel(SXSSFWorkbook workbook){
		String[] titles = {"", "", "", ""};
		SXSSFSheet sheet = null;
		SXSSFRow rowIndex = null;
		sheet = workbook.createSheet("Warehouse address");
		rowIndex = sheet.createRow(0);
		for (int i = 0; i < titles.length; i++) {
			rowIndex.createCell(i).setCellValue(titles[i]);
		}

		int index = 0;
		int count = 1;
		for (int i = 0; i < 25; i++) {
			index = 0;
			rowIndex = sheet.createRow(count);
			if(count < 4){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
			}
			else if(count == 4){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Warehouse");
				rowIndex.createCell(index++).setCellValue("Address");
			}
			else if(count == 5){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("USA");
				rowIndex.createCell(index++).setCellValue("");
			}
			else if(count == 6){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Houston");
				rowIndex.createCell(index++).setCellValue("22745 NW Lake Dr Houston, TX 77095, United States");
			}
			else if(count == 7){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Rancho");
				rowIndex.createCell(index++).setCellValue("10270 Philadelphia Court, Rancho Cucamonga, CA 91730, United States");
			}
			else if(count == 8){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Atlanta");
				rowIndex.createCell(index++).setCellValue("7625 Southlake Pkwy Jonesboro, GA 30236, United States");
			}
			/*else if(count == 9){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Delta");
				rowIndex.createCell(index++).setCellValue("9410 River Road, Delta, BC V4G 1B5, Canada");
			}*/
			else if(count == 9){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Ontario");
				rowIndex.createCell(index++).setCellValue("1800 S Milliken Ave Ontario, CA 91761, United States");
			}
			else if(count == 10){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US LSQ");
				rowIndex.createCell(index++).setCellValue("3095-200 E Cedar St, Ontario, CA 91761, United States");
			}
			else if(count == 11){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US ONO");
				rowIndex.createCell(index++).setCellValue("2151 S Proforma Ave, Ontario, CA 91761, United States");
			}
			else if(count == 12){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US CHA");
				rowIndex.createCell(index++).setCellValue("2145 Anvil Block Rd, Suite 200, Ellenwood, GA 30294, United States");
			}
			else if(count == 13){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US FBO");
				rowIndex.createCell(index++).setCellValue("Paris Loft Inc, 1332 Stonefield Ct, Alpharetta, GA 30004, United States");
			}
			else if(count == 14){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US PDK");
				rowIndex.createCell(index++).setCellValue("Douglas Hill Park3, 767 Douglas hill road, Lithia Springs, GA, 30122, United States");
			}
			else if(count == 15){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US GSP");
				rowIndex.createCell(index++).setCellValue("154 Metro Ct, Greer, SC 29650, United States");
			}
			else if(count == 16){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US GDC");
				rowIndex.createCell(index++).setCellValue("400 Bon Air St, Mauldin, SC 29662, United States");
			}
			else if(count == 17){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US PHL");
				rowIndex.createCell(index++).setCellValue("520 Pedricktown Rd, Bridgeport, NJ 08014, United States");
			}
			else if(count == 18){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("US AUS");
				rowIndex.createCell(index++).setCellValue("5615 W. Fuqua St. STE C-100, Dock 1-10  Houston, TX 77085, United States");
			}
			else if(count == 19){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Canada");
				rowIndex.createCell(index++).setCellValue("");
			}
			else if(count == 20){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Delta");
				rowIndex.createCell(index++).setCellValue("9410 River Road, Delta, BC V4G 1B5, Canada");
			}
			else if(count == 21){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Toronto");
				rowIndex.createCell(index++).setCellValue("50 East Wilmot St, Richmond Hill, ON L4B 3Z3, Canada");
			}
			else if(count == 22){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("Richmond Hill");
				rowIndex.createCell(index++).setCellValue("60 East Beaver Creek RdRichmond Hill, ON L4B 1L3, Canada");
			}
			else if(count == 23){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("CA YKZ");
				rowIndex.createCell(index++).setCellValue("6620 Kestrel Road, Mississauga, ON L5T 2C8, Canda");
			}
			else if(count == 24){
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("");
				rowIndex.createCell(index++).setCellValue("CA YMX");
				rowIndex.createCell(index++).setCellValue("7248 Rue Cordner, LaSalle, QC H8N 2W8, Canada");
			}
			
			count++;
		}
		
		
		return workbook;
	}

	@Override
	public Map<String, Object> pageInitOnlineInventory() {
		// TODO 线上库存初始化页面
		List<String> warehouseCountryList = new ArrayList<>();
		warehouseCountryList.add("CA");
		warehouseCountryList.add("US");
		warehouseCountryList.add(0, null);
		
		Map<String, Object> resMap = new HashMap<String, Object>();
		resMap.put("flag", true);
		resMap.put("aquery", new OnlineInventoryQuery());
		resMap.put("pageDomain", new PageDomain());
		resMap.put("usFlag", false);
		resMap.put("caFlag", false);
		resMap.put("warehouseCountryList", warehouseCountryList);
		return resMap;
	}
	
	@Override
	public Map<String, Object> selectOnlineInventoryByPage(OnlineInventoryQuery aquery, PageDomain pageDomain) {
		// TODO 条件查询
		Map<String, Object> resMap = new HashMap<>();
		
		if(aquery.getCountryName() != null) {
			
			ObjectMapper mapper = new ObjectMapper();
			/** 访问URL */
			PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.TH_DOWN_NO);
			String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
			logger.info("太华补货表库存接口地址 url : {}", url);
			//参数
			KaReplChartQuery replQuery = new KaReplChartQuery();
			
			List<FourUnitMerchantRepl> thStockList = new ArrayList<>();
			String jsonParam;
			try {
				jsonParam = JsonUtils.objToJson(replQuery);
				Map<String, String> headermap = new HashMap<>();
				headermap.put("Content-Type", "application/json;charset=utf-8");
				Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
				String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
				if("200".equals(code)) {//成功
					String responseMsg = (String)resultStockMap.get("responseMsg");
					thStockList = mapper.readValue(responseMsg, new TypeReference<List<FourUnitMerchantRepl>>(){});
				}else {//失败
					logger.info("error : {}", resultStockMap.get("responseMsg")); 
					resMap.put("flag", false);
					resMap.put("msg", "查询线上库存接口失败："+resultStockMap.get("responseMsg"));
					return resMap;
				}
			}catch (Exception e) {
				logger.info("error : {}", "查询线上库存接口失败："+e); 
				resMap.put("flag", false);
				resMap.put("msg", "查询线上库存接口失败："+e.getMessage());
				return resMap;
			}
			logger.info("thStockList size : {}", thStockList.size());
			
			
			//20221205 过滤，汇总
			thStockList = getSumReplData(thStockList);
			
			List<FourUnitMerchantRepl> thUSStockList = new ArrayList<>();	
			List<FourUnitMerchantRepl> thCAStockList = new ArrayList<>();		
			for (FourUnitMerchantRepl replDetail : thStockList) {
				String countryCode = replDetail.getCountry() == null ? "" : replDetail.getCountry();
				if(countryCode.equalsIgnoreCase("US")){
					thUSStockList.add(replDetail);
				}
				else if(countryCode.equalsIgnoreCase("CA")){
					thCAStockList.add(replDetail);
				}
			}
			int usCount = thUSStockList.size();
			int caCount = thCAStockList.size();
			logger.info("thUSStockList size : {}", usCount);
			logger.info("thCAStockList size : {}", caCount);
		
			if(aquery.getCountryName().equals("US")) {
				//20221205 排序汇总 US
				List<OnlineInventory> onlineUSList = getSortUSData(thUSStockList);
				//分页 //US
				onlineUSList = ListUtil.datepaging(onlineUSList, pageDomain.getPageIndex(), pageDomain.getPageSize());
				
				resMap.put("usFlag", true);
				resMap.put("caFlag", false);
				resMap.put("datalist", onlineUSList);
				pageDomain.setRecordCount(usCount);
			}
			else if(aquery.getCountryName().equals("CA")) {
				//20221205 排序汇总 CA
				List<OnlineInventory> onlineCAList = getSortCAData(thCAStockList);
				//CA
				onlineCAList = ListUtil.datepaging(onlineCAList, pageDomain.getPageIndex(), pageDomain.getPageSize());
				
				resMap.put("usFlag", false);
				resMap.put("caFlag", true);
				resMap.put("datalist", onlineCAList);
				pageDomain.setRecordCount(caCount);
			}
		}else {
			resMap.put("usFlag", false);
			resMap.put("caFlag", false);
			resMap.put("datalist", new ArrayList<>());
		}
		resMap.put("flag", true);
		resMap.put("pageDomain", pageDomain);
		
		return resMap;
	}

	private List<OnlineInventory> getSortUSData(List<FourUnitMerchantRepl> thUSStockList){
		List<OnlineInventory> onlineUSList = new ArrayList<>();
		for (int i = 0; i < thUSStockList.size(); i++) {
			FourUnitMerchantRepl replDetail = thUSStockList.get(i);
			
			OnlineInventory onlineInventory = new OnlineInventory();
			BeanUtils.copyProperties(replDetail, onlineInventory);
			
			//Rancho(仓1)
			Integer rancho = replDetail.getWarehouseOne() == null ? 0 : replDetail.getWarehouseOne();
			//Ontario(仓2)
			Integer ontario = replDetail.getWarehouseTwo() == null ? 0 : replDetail.getWarehouseTwo();
			//Atlanta(仓3)
			Integer atlanta = replDetail.getWarehouseThree() == null ? 0 : replDetail.getWarehouseThree();
			//Houston(优衣库(Houston))
			Integer houston = replDetail.getHouston() == null ? 0 : replDetail.getHouston();
			//US CCP(齐物(US CCP))
			Integer usCcp = replDetail.getUsCcp() == null ? 0 : replDetail.getUsCcp();
			//US LSQ(天道(US LSQ))
			Integer usLsq = replDetail.getUsLsq() == null ? 0 : replDetail.getUsLsq();
			//US GSP(人间(US GSP))
			Integer usGsp = replDetail.getUsGsp() == null ? 0 : replDetail.getUsGsp();
			//US CHA(月令(US CHA))
			Integer usCha = replDetail.getUsCha() == null ? 0 : replDetail.getUsCha();
			//US ATL(天下(US ATL))
			Integer usAtl = replDetail.getUsAtl() == null ? 0 : replDetail.getUsAtl();
			//US ONO(北游(US ONO))
			Integer usOno = replDetail.getUsOno() == null ? 0 : replDetail.getUsOno();
			//US SNA(说剑(US SNA))
			Integer usSna = replDetail.getUsSna() == null ? 0 : replDetail.getUsSna();
			//US BUR(达生(US BUR))
			Integer usBur = replDetail.getUsBur() == null ? 0 : replDetail.getUsBur();
			//US PDK(桑楚(US PDK))
			Integer usPdk = replDetail.getUsPdk() == null ? 0 : replDetail.getUsPdk();
			//US PHL(天运(US PHL))
			Integer usPhl = replDetail.getUsPhl() == null ? 0 : replDetail.getUsPhl();

			int stockAllUS = rancho + ontario + atlanta + houston + usCcp + usLsq + usGsp + usCha + usAtl + usOno 
								+ usSna + usBur + usPdk + usPhl;
			
			int serNumber = i + 1;
			onlineInventory.setNumber(serNumber);
			onlineInventory.setTotalAbleQty(stockAllUS);
			
			onlineUSList.add(onlineInventory);
		}
		
		return onlineUSList;
	}
	
	private List<OnlineInventory> getSortCAData(List<FourUnitMerchantRepl> thCAStockList){
		List<OnlineInventory> onlineUSList = new ArrayList<>();
		for (int i = 0; i < thCAStockList.size(); i++) {
			FourUnitMerchantRepl replDetail = thCAStockList.get(i);
			
			OnlineInventory onlineInventory = new OnlineInventory();
			BeanUtils.copyProperties(replDetail, onlineInventory);
			
			//Toronto(仓1)
			Integer toronto = replDetail.getWarehouseOne() == null ? 0 : replDetail.getWarehouseOne();
			//RICHMOND HILL(吉野家(RICHMOND HILL))
			Integer richmondHill = replDetail.getRichmondHill() == null ? 0 : replDetail.getRichmondHill();
			//YUL(问辩(CA YUL))
			Integer caYul = replDetail.getCaYul() == null ? 0 : replDetail.getCaYul();
			//YTZ(逍遥(CA YTZ))
			Integer caYtz = replDetail.getCaYtz() == null ? 0 : replDetail.getCaYtz();
			//Delta(三角洲(Delta))
			Integer delta = replDetail.getDelta() == null ? 0 : replDetail.getDelta();
			//YMX(说疑(CA YMX))
			Integer caYmx = replDetail.getCaYmx() == null ? 0 : replDetail.getCaYmx();

			int stockAllCA = toronto + richmondHill + caYul + caYtz + delta + caYmx;
			int serNumber = i + 1;
			onlineInventory.setNumber(serNumber);
			onlineInventory.setTotalAbleQty(stockAllCA);
			
			onlineUSList.add(onlineInventory);
		}
		
		return onlineUSList;
	}

	@Override
	public void downloadXWRepl(Context context, HttpServletResponse response) {
		// TODO 20230322 lillian 下载晓望调价监控表
		/** 访问URL */
		/*ObjectMapper mapper = new ObjectMapper();
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.FOUR_REPL_DOWN_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("四字机构补货表库存接口地址 url : {}", url);*/
		//参数
		KaReplChartQuery replQuery = new KaReplChartQuery();
		
		List<String> fourUnitList = new ArrayList<>();
		//晓望
		fourUnitList.add("太华天街");
		fourUnitList.add("晓望梅观");
		fourUnitList.add("龙门客栈");
		//古月
		fourUnitList.add("水墨江南");
		fourUnitList.add("奇诺山庄");
		fourUnitList.add("渔人码头");
		fourUnitList.add("和平饭店");
		replQuery.setFourUnitList(fourUnitList);
		replQuery.setAdate(new Date());//取最新的补货表数据
		
		/*List<FourUnitMerchantRepl> thStockList = new ArrayList<>();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				thStockList = mapper.readValue(responseMsg, new TypeReference<List<FourUnitMerchantRepl>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "四字机构补货表库存接口失败："+e); 
		}
		logger.info("thStockList size : {}", thStockList.size());*/
		
		List<FourUnitMerchantRepl> thStockList = getReplMerchantStockData(replQuery);
		
		//查询SKU系列映射数据
		Map<String, KaSkuSeriesMapping> skuSeriesMappingByNameMap = getSkuSeriesMappingData();
		
		/** 订单大表和成本数据 */
		//获取昨日订单与成本
		Map<String, Object> costResultMap = 
				getYesterdayOrder(replQuery, fourUnitList, skuSeriesMappingByNameMap);
		Map<String, FourUnitSales> skuSalesMap = (Map<String, FourUnitSales>) costResultMap.get("skuSalesMap");
		Map<String, FourUnitSales> categorySalesMap = (Map<String, FourUnitSales>) costResultMap.get("categorySalesMap");
		logger.info("skuSalesMap size : {}", skuSalesMap.size());
		logger.info("categorySalesMap size : {}", categorySalesMap.size());
		/** 20230331 汇总平台对应的收入和利润 */
		Map<String, Map<String, FourUnitSales>> platSkuResultMap = 
				(Map<String, Map<String, FourUnitSales>>) costResultMap.get("platSkuResultMap");
		Map<String, Map<String, FourUnitSales>> platCategoryResultMap = 
				(Map<String, Map<String, FourUnitSales>>) costResultMap.get("platCategoryResultMap");
		logger.info("platSkuResultMap size : {}", platSkuResultMap.size());
		logger.info("platCategoryResultMap size : {}", platCategoryResultMap.size());
		
		/** 补货表数据 */
		//SKU
		List<FourUnitReplXW> skuReplList = getXWSumReplData(thStockList, 1, skuSalesMap, 
				platSkuResultMap, skuSeriesMappingByNameMap);
		//款式
		List<FourUnitReplXW> categoryReplList = getXWSumReplData(thStockList, 2, categorySalesMap, 
				platCategoryResultMap, skuSeriesMappingByNameMap);
		
		//20230329 根据国家和集群汇总
		Map<String, List<FourUnitReplXW>> colonySKUResultMap = dealDataWithColonyCountryMap(skuReplList);
		Map<String, List<FourUnitReplXW>> colonyCategoryResultMap = dealDataWithColonyCountryMap(categoryReplList);
		
		//生成excel
		OutputStream os = null;
		String pathName = "调价监控表.xls";//
		SXSSFWorkbook workbook = new SXSSFWorkbook(500);
		
		/*buildSKUExcel(workbook, skuReplList);
		workbook = buildCategoryExcel(workbook, categoryReplList);*/
		for (Entry<String, List<FourUnitReplXW>> skuResultEntry : colonySKUResultMap.entrySet()) {
			String key = skuResultEntry.getKey();
			List<FourUnitReplXW> skuSubList = skuResultEntry.getValue();
			
			List<FourUnitReplXW> categorySubList = new ArrayList<>();
			if(colonyCategoryResultMap.containsKey(key)) {
				categorySubList = colonyCategoryResultMap.get(key);
			}
			
			String colonyKey = key.replace(",", "-");
			//创建excel
			buildSKUExcel(workbook, skuSubList, colonyKey);
			workbook = buildCategoryExcel(workbook, categorySubList, colonyKey);
		}
		
		try {
            response.reset();
            response.setContentType("application/vnd.ms-excel;charset=UTF-8");
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Disposition", "inline; filename=" + pathName);
            response.setHeader("fname", pathName);
            response.setHeader("flag", "1");
            os = response.getOutputStream();
            workbook.write(os);
            os.flush();
        } catch (Exception e) {
            logger.info("exportExcel", e);
            response.setHeader("flag", "0");
            response.setHeader("msg", e.getMessage());
        } finally {
            try {
                if (os != null)
                    os.close();
                workbook.close();
            } catch (IOException e) {
                logger.info("exportExcel close os or wb", e);
            }
        }
		logger.info("调价监控表 Download finish!");
	}
	
	
	/** 20230329 区分集群和国家 */
	private Map<String, List<FourUnitReplXW>> dealDataWithColonyCountryMap(List<FourUnitReplXW> replList) {
		Map<String, List<FourUnitReplXW>> colonyResultMap = new HashMap<>();
		List<FourUnitReplXW> colonyResultList = new ArrayList<>();
		
		if(replList != null && replList.size() > 0) {
			/*
			 * 20230329 在原有模板的基础上，数据按照四字机构+国家分sheet导出，其中太华天街+晓望梅观合并起来以国家分sheet展示，
			 * 	例如：水墨江南us在一个页签中显示，太华天街us+晓望梅观us在一个页签中展示
			 */
			Map<String, String> fourunitForColonyMap = new HashMap<>();
			//晓望
			fourunitForColonyMap.put("太华天街", "晓望");
			fourunitForColonyMap.put("晓望梅观", "晓望");
			fourunitForColonyMap.put("龙门客栈", "晓望");
			//古月
			fourunitForColonyMap.put("水墨江南", "古月");
			fourunitForColonyMap.put("奇诺山庄", "古月");
			fourunitForColonyMap.put("渔人码头", "古月");
			fourunitForColonyMap.put("和平饭店", "古月");
			
			for (FourUnitReplXW fourUnitReplXW : replList) {
				String fourUnit = fourUnitReplXW.getFourUnit();
				String countryCode = fourUnitReplXW.getCountryCode();
				String colonyName = fourUnit;
				if(fourunitForColonyMap.containsKey(fourUnit)) {
					colonyName = fourunitForColonyMap.get(fourUnit);
				}
				
				String colonyKey = colonyName + "," + countryCode;
				if(colonyResultMap.containsKey(colonyKey)) {
					colonyResultList = colonyResultMap.get(colonyKey);
				}else {
					colonyResultList = new ArrayList<>();
					colonyResultMap.put(colonyKey, colonyResultList);
				}
				colonyResultList.add(fourUnitReplXW);
			}
		}
		
		return colonyResultMap;
	}
	
	
	
	private Map<String, KaSkuSeriesMapping> getSkuSeriesMappingData() {
		// TODO 查询SKU系列映射数据
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.SKU_SERIES_MAPPING_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("查询SKU系列映射数据地址 url : {}", url);
		
		KaReplChartQuery replQuery = new KaReplChartQuery();
		List<KaSkuSeriesMapping> allSkuSeriesMappingList = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				allSkuSeriesMappingList = mapper.readValue(responseMsg, new TypeReference<List<KaSkuSeriesMapping>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "查询SKU系列映射数据接口失败："+e); 
		}
		logger.info("allSkuSeriesMappingList size : {}", allSkuSeriesMappingList.size());
		
		Map<String, KaSkuSeriesMapping> skuSeriesMappingByNameMap = new HashMap<>();
		if(allSkuSeriesMappingList != null && allSkuSeriesMappingList.size() > 0) {
			for (KaSkuSeriesMapping kaSkuSeriesMapping : allSkuSeriesMappingList) {
				String goodsName = kaSkuSeriesMapping.getGoodsName().trim();
				if(!skuSeriesMappingByNameMap.containsKey(goodsName)) {
					skuSeriesMappingByNameMap.put(goodsName, kaSkuSeriesMapping);
				}
			}
		}
		return skuSeriesMappingByNameMap;
	}

	private Map<String, Object> getYesterdayOrder(KaReplChartQuery replQuery, List<String> fourUnitList,
			Map<String, KaSkuSeriesMapping> skuSeriesMappingByNameMap) {
		// TODO 获取订单大表昨日订单与成本
		Map<String, Object> costResultMap = new HashMap<>();
		
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.FOUR_REPL_ORDER_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("查询订单大表数据且匹配成本接口地址 url : {}", url);
		
		
		List<FourUnitSales> allOrderStockList = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				allOrderStockList = mapper.readValue(responseMsg, new TypeReference<List<FourUnitSales>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "查询订单大表数据且匹配成本接口失败："+e); 
		}
		logger.info("allOrderStockList size : {}", allOrderStockList.size());
		
		List<FourUnitSales> thSkuOrderStockList = new ArrayList<>();
		List<FourUnitSales> thCategoryOrderStockList = new ArrayList<>();
		if(allOrderStockList != null && allOrderStockList.size() > 0) {
			//查询
			List<ErpCloudMaterialAllDto> coreAndSubArticleList = erpCloudMaterialNumApi.selectAllByCoreAndSubArticle();
			//ERP-云库存关联货号
	        Map<String, ErpCloudMaterialAllDto> coreAndSubArticleSkuMap = 
	        		Maps.uniqueIndex(coreAndSubArticleList, ErpCloudMaterialAllDto::getSubArticleSku);
	        logger.info("coreAndSubArticleSkuMap size : {}", coreAndSubArticleSkuMap.size());
	        //HIB-四字机构关联货号
	        List<OrganizationRelativeMerchandiseDto> hibORMaterialList = organizationRelativeMerchandiseApi.selectAll();
	        Map<String, OrganizationRelativeMerchandiseDto> hibFourUnitAssociatedMaterialMap = new HashMap<>();
	        for (OrganizationRelativeMerchandiseDto allDto : hibORMaterialList) {
	        	hibFourUnitAssociatedMaterialMap.put(allDto.getArelativeMerchandise().getAnumber(), allDto);
	        }
	        logger.info("hibFourUnitAssociatedMaterialMap size : {}", hibFourUnitAssociatedMaterialMap.size());
			
			for (FourUnitSales fourUnitSales : allOrderStockList) {
				if(fourUnitSales.getFourUnit() != null && fourUnitList.contains(fourUnitSales.getFourUnit())) {
					String fourUnit = fourUnitSales.getFourUnit();
					//处理核心货号
					String productNumber = fourUnitSales.getProductNumber();
					if (productNumber != null && "0000000000000".equals(productNumber)) {
						continue;
					}
					String productName = fourUnitSales.getProductName();
					String categoryEnglishName = fourUnitSales.getCategoryEnglishName();
					//判断是否为核心或号
	                if (coreAndSubArticleSkuMap.containsKey(productNumber)) {
	                    ErpCloudMaterialAllDto erpCloudMaterialAllDto = coreAndSubArticleSkuMap.get(productNumber);
	                    productNumber = erpCloudMaterialAllDto.getCoreSku();
	                    productName = erpCloudMaterialAllDto.getCoreEngname();
	                    //categoryEnglishName = erpCloudMaterialAllDto.getCoreGroupEngname();
	                }
	                
	                //HIB-四字机构关联货号
	                if (hibFourUnitAssociatedMaterialMap.containsKey(productNumber)) {
	                	OrganizationRelativeMerchandiseDto hibORMaterialDto = hibFourUnitAssociatedMaterialMap.get(productNumber);
	                    String hibFourUnit = hibORMaterialDto.getFourOrganization() == null ? "" : hibORMaterialDto.getFourOrganization().getAname();
	                    if (hibFourUnit.equals(fourUnit)) {
	                        //四字机构一致
	                        productNumber = hibORMaterialDto.getAmainMerchandise().getAnumber();
	                        productName = hibORMaterialDto.getAmainMerchandise().getAname();
	                        categoryEnglishName = hibORMaterialDto.getAmainMerchandise().getBaseCategory() == null ? 
	                        		categoryEnglishName : hibORMaterialDto.getAmainMerchandise().getBaseCategory().getAengname(); 
	                    }
	                }
	                
	                if(skuSeriesMappingByNameMap.containsKey(productName)) {
						//匹配SKU系列映射
						KaSkuSeriesMapping kaSkuSeriesMapping = skuSeriesMappingByNameMap.get(productName);
						fourUnitSales.setSuccession(kaSkuSeriesMapping.getSuccession());
					}
	                
	                fourUnitSales.setProductName(productName);
	                fourUnitSales.setProductNumber(productNumber);
	                fourUnitSales.setCategoryEnglishName(categoryEnglishName);
	                
					thSkuOrderStockList.add(fourUnitSales);
				}
			}
		}
		thCategoryOrderStockList.addAll(thSkuOrderStockList);
		logger.info("thOrderStockList size : {}", thSkuOrderStockList.size());
		
		Map<String, FourUnitSales> skuSalesMap = getXWSumOrderData(thSkuOrderStockList, 1);
		Map<String, FourUnitSales> categorySalesMap = getXWSumOrderData(thCategoryOrderStockList, 2);
		costResultMap.put("skuSalesMap", skuSalesMap);
		costResultMap.put("categorySalesMap", categorySalesMap);
		
		/** 20230331 汇总平台对应的收入和利润 */
		Map<String, Map<String, FourUnitSales>> platSkuResultMap = getXWSumPlatformOrderData(thSkuOrderStockList, 1);
		Map<String, Map<String, FourUnitSales>> platCategoryResultMap = getXWSumPlatformOrderData(thCategoryOrderStockList, 2);
		costResultMap.put("platSkuResultMap", platSkuResultMap);
		costResultMap.put("platCategoryResultMap", platCategoryResultMap);
		
		return costResultMap;
	}
	
	private Map<String, FourUnitSales> getXWSumOrderData(List<FourUnitSales> thOrderStockList, Integer sumType) {
		// TODO 过滤，汇总数据
		Map<String, FourUnitSales> replBySumKeyMap = new HashMap<>();
		FourUnitSales newSales = new FourUnitSales();
		
       
        for (FourUnitSales unitSales : thOrderStockList) {
        	
        	//20230327 2、导出模板中过滤掉Sub-Category取值为spare part的数据
        	String category = unitSales.getCategoryEnglishName() == null ? "" : unitSales.getCategoryEnglishName();
			if(category.equalsIgnoreCase("Spare Parts")){
				continue;
			}
        	
        	String succession = unitSales.getSuccession() == null ? "" : unitSales.getSuccession();//系列
        	BigDecimal saleYesterdayQty = NumberUtil.getBigDecimal(unitSales.getSaleYesterdayQty());//昨天销量
        	BigDecimal saleYesterdayAmount = NumberUtil.getBigDecimal(unitSales.getSaleYesterdayAmount());//营业额 
        	BigDecimal saleYesterdayCost = NumberUtil.getBigDecimal(unitSales.getSaleYesterdayCost());//总成本
			
			//系列 + SKU + 四字机构 + 国家
			String sumKey = succession + "," + unitSales.getProductNumber() + "," + unitSales.getFourUnit() 
				+ "," + unitSales.getSiteName();
			if(sumType.equals(2)) {
				//系列 + 类别 + 四字机构 + 国家
				sumKey = succession + "," + unitSales.getCategoryEnglishName() + "," + unitSales.getFourUnit() 
					+ "," + unitSales.getSiteName();
			}
			if(replBySumKeyMap.containsKey(sumKey)) {
				newSales = replBySumKeyMap.get(sumKey);
				
				newSales.setSaleYesterdayQty(
						saleYesterdayQty.add(NumberUtil.getBigDecimal(newSales.getSaleYesterdayQty())).intValue());
				newSales.setSaleYesterdayAmount(
						saleYesterdayAmount.add(NumberUtil.getBigDecimal(newSales.getSaleYesterdayAmount())));
				newSales.setSaleYesterdayCost(
						saleYesterdayCost.add(NumberUtil.getBigDecimal(newSales.getSaleYesterdayCost())));
			} else {
				newSales = new FourUnitSales();
				BeanUtils.copyProperties(unitSales, newSales);
				replBySumKeyMap.put(sumKey, newSales);
			}
			
		}
        return replBySumKeyMap;
	}
	
	/** 获取平台数据 */
	private Map<String, Map<String, FourUnitSales>> getXWSumPlatformOrderData(List<FourUnitSales> thOrderStockList, Integer sumType) {
		// TODO 过滤，汇总数据
		Map<String, Map<String, FourUnitSales>> platformResultMap = new HashMap<>();
		Map<String, FourUnitSales> replBySumKeyMap = new HashMap<>();
		FourUnitSales newSales = new FourUnitSales();
		
       
        for (FourUnitSales unitSales : thOrderStockList) {
        	
        	//20230327 2、导出模板中过滤掉Sub-Category取值为spare part的数据
        	String category = unitSales.getCategoryEnglishName() == null ? "" : unitSales.getCategoryEnglishName();
			if(category.equalsIgnoreCase("Spare Parts")){
				continue;
			}
        	String platformName = unitSales.getPlatformName() == null ? "" : unitSales.getPlatformName().toUpperCase();//平台大写
        	String siteName = unitSales.getSiteName() == null ? "" : unitSales.getSiteName();//平台大写
        	String succession = unitSales.getSuccession() == null ? "" : unitSales.getSuccession();//系列
        	BigDecimal saleYesterdayQty = NumberUtil.getBigDecimal(unitSales.getSaleYesterdayQty());//昨天销量
        	BigDecimal saleYesterdayAmount = NumberUtil.getBigDecimal(unitSales.getSaleYesterdayAmount());//营业额 
        	BigDecimal saleYesterdayCost = NumberUtil.getBigDecimal(unitSales.getSaleYesterdayCost());//总成本
			
			//系列 + SKU + 四字机构 + 国家
			String sumKey = succession + "," + unitSales.getProductNumber() + "," + unitSales.getFourUnit() 
				+ "," + unitSales.getSiteName();
			if(sumType.equals(2)) {
				//系列 + 类别 + 四字机构 + 国家
				sumKey = succession + "," + unitSales.getCategoryEnglishName() + "," + unitSales.getFourUnit() 
					+ "," + unitSales.getSiteName();
			}
			
			//平台KEY
			String platformKey = platformName;
			if(platformName.indexOf("OVERSTOCK") != -1) {//Overstock
				platformKey = "OVERSTOCK";
			} else if(platformName.indexOf("WAYFAIR") != -1) {//Wayfair
				platformKey = "WAYFAIR" + "," + siteName;
			}  else if(platformName.indexOf("WALMART") != -1) {//Walmart(Walmart_DSV、Walmart_MP)
				platformKey = "WALMART" + "," + siteName;
			}  else if(platformName.indexOf("HOME_DEPOT") != -1) {//The Home Depot(HOME_DEPOT.CA、Home_Depot)
				platformKey = "HOME_DEPOT";
			}  else if(platformName.indexOf("AMAZON") != -1) {//Amazon
				platformKey = platformName + "," + siteName;
			}  
			
			
			if(platformResultMap.containsKey(sumKey)) {
				replBySumKeyMap = platformResultMap.get(sumKey);
				if(replBySumKeyMap.containsKey(platformKey)) {
					newSales = replBySumKeyMap.get(platformKey);
					
					newSales.setSaleYesterdayQty(
							saleYesterdayQty.add(NumberUtil.getBigDecimal(newSales.getSaleYesterdayQty())).intValue());
					newSales.setSaleYesterdayAmount(
							saleYesterdayAmount.add(NumberUtil.getBigDecimal(newSales.getSaleYesterdayAmount())));
					newSales.setSaleYesterdayCost(
							saleYesterdayCost.add(NumberUtil.getBigDecimal(newSales.getSaleYesterdayCost())));
				}else {
					newSales = new FourUnitSales();
					BeanUtils.copyProperties(unitSales, newSales);
					replBySumKeyMap.put(platformKey, newSales);
				}
			}else {
				replBySumKeyMap = new HashMap<>();
				newSales = new FourUnitSales();
				BeanUtils.copyProperties(unitSales, newSales);
				replBySumKeyMap.put(platformKey, newSales);
				platformResultMap.put(sumKey, replBySumKeyMap);
			}
			
		}
        return platformResultMap;
	}

	private SXSSFWorkbook buildCategoryExcel(SXSSFWorkbook workbook, List<FourUnitReplXW> categoryReplList,
			String colonyKey) {
		// TODO 晓望调价监控表excel - Category

		/*String[] titles = {"系列", "Sub-Category", "当天在仓库存量", "前天销量", "昨天销量", "当天周转天数", "前14-前8天销量", "前7天销量",
				"销售变化 (增长、下降、不变)", "定价利润 (盈利/亏损)", "晓望当天总营业额 (USD)", "晓望当天总成本 (USD)", "晓望当天总利润 (USD)", 
				"Wayfair 零售价", "Wayfair 竞品零售价", "Amazon 零售价", "Amazon 竞品零售价", "Walmart 零售价", "Walmart 竞品零售价",
				"The Home Depot 零售价", "The Home Depot 竞品零售价", "Overstock 零售价", "Overstock竞品零售价", "四字机构","国家"};*/
		String[] titles = {"款式", "Sub-Category", "当天在仓库存量", "前天销量", "昨天销量", "当天周转天数", "前14-前8天销量", "前7天销量",
				"销售变化 (增长、下降、不变)", "晓望当天总营业额 (USD)", "晓望当天总成本 (USD)",
				"晓望7000运费总利润 (USD)","晓望7000运费利润率","定价利润 (盈利/亏损)",
				"晓望到箱总利润 (USD)", "晓望到箱利润率", "定价利润 (盈利/亏损)",  
				"Wayfair US 零售价", "链接首图","零售价更新日期","Wayfair US利润率","Wayfair US竞品零售价", "竞品首图",
				"Wayfair CA零售价", "链接首图","零售价更新日期","Wayfair CA利润率","Wayfair 竞品零售价", "竞品首图",
				"Amazon CA零售价", "链接首图","零售价更新日期","Amazon CA利润率","Amazon 竞品零售价", "竞品首图",
				"Walmart US零售价", "链接首图","零售价更新日期","Walmart US利润率","Walmart 竞品零售价","竞品首图",
				"Walmart CA零售价", "链接首图","零售价更新日期","Walmart CA利润率","Walmart 竞品零售价","竞品首图",
				"The Home Depot 零售价", "链接首图","零售价更新日期","The Home Depot 利润率","The Home Depot 竞品零售价", "竞品首图",
				"Overstock 零售价", "链接首图","零售价更新日期","Overstock 利润率","Overstock竞品零售价", "竞品首图",
				"四字机构","国家"};
		workbook = buildXWExcel(workbook, colonyKey, categoryReplList, 2, titles);
		return workbook;
	
	}

	private SXSSFWorkbook buildSKUExcel(SXSSFWorkbook workbook, List<FourUnitReplXW> skuReplList,
			String colonyKey) {
		// TODO 晓望调价监控表excel - SKU
		/*String[] titles = {"系列", "HIB SKU", "Sub-Category", "当天在仓库存量", "前天销量", "昨天销量", "当天周转天数", "前14-前8天销量", "前7天销量",
				"销售变化 (增长、下降、不变)", "定价利润 (盈利/亏损)", "晓望当天总营业额 (USD)", "晓望当天总成本 (USD)", "晓望当天总利润 (USD)", 
				"Wayfair 零售价", "Wayfair 竞品零售价", "Amazon 零售价", "Amazon 竞品零售价", "Walmart 零售价", "Walmart 竞品零售价",
				"The Home Depot 零售价", "The Home Depot 竞品零售价", "Overstock 零售价", "Overstock竞品零售价", "四字机构","国家"};*/
		String[] titles = {"款式", "HIB SKU", "Sub-Category", "当天在仓库存量", "前天销量", "昨天销量", "当天周转天数", "前14-前8天销量", "前7天销量",
				"销售变化 (增长、下降、不变)", "晓望当天总营业额 (USD)", "晓望当天总成本 (USD)",
				"晓望7000运费总利润 (USD)","晓望7000运费利润率","定价利润 (盈利/亏损)",
				"晓望到箱总利润 (USD)", "晓望到箱利润率", "定价利润 (盈利/亏损)",  
				"Wayfair US 零售价", "链接首图","零售价更新日期","Wayfair US利润率","Wayfair US竞品零售价", "竞品首图",
				"Wayfair CA零售价", "链接首图","零售价更新日期","Wayfair CA利润率","Wayfair 竞品零售价", "竞品首图",
				"Amazon CA零售价", "链接首图","零售价更新日期","Amazon CA利润率","Amazon 竞品零售价", "竞品首图",
				"Walmart US零售价", "链接首图","零售价更新日期","Walmart US利润率","Walmart 竞品零售价","竞品首图",
				"Walmart CA零售价", "链接首图","零售价更新日期","Walmart CA利润率","Walmart 竞品零售价","竞品首图",
				"The Home Depot 零售价", "链接首图","零售价更新日期","The Home Depot 利润率","The Home Depot 竞品零售价", "竞品首图",
				"Overstock 零售价", "链接首图","零售价更新日期","Overstock 利润率","Overstock竞品零售价", "竞品首图",
				"四字机构","国家"};
		workbook = buildXWExcel(workbook, colonyKey, skuReplList, 1, titles);
		return workbook;
	}
	
	private SXSSFWorkbook buildXWExcel(SXSSFWorkbook workbook, String colonyKey, List<FourUnitReplXW> xwReplList,
			Integer sumType, String[] titles) {
		// TODO 晓望调价监控表excel
		SXSSFSheet sheet = null;
		if(sumType.equals(2)) {
			sheet = workbook.createSheet(colonyKey + "-" + "款式");
		}else {
			sheet = workbook.createSheet(colonyKey + "-" + "SKU");
		}
		
		SXSSFRow rowIndex = null;
		rowIndex = sheet.createRow(0);
		for (int i = 0; i < titles.length; i++) {
			rowIndex.createCell(i).setCellValue(titles[i]);
		}

		int index = 0;
		int count = 1;
		if (xwReplList != null && xwReplList.size() > 0) {
			for (FourUnitReplXW replDetail : xwReplList) {
				index = 0;
				rowIndex = sheet.createRow(count);
				
				//系列
				rowIndex.createCell(index++).setCellValue(replDetail.getSuccession());
				
				if(!sumType.equals(2)) {
					//HIB SKU
					//rowIndex.createCell(index++).setCellValue(replDetail.getSku());
					rowIndex.createCell(index++).setCellValue(replDetail.getHibSkuName());
				}
				
				//Sub-Category
				rowIndex.createCell(index++).setCellValue(replDetail.getCategory());
				//当天在仓库存量
				if(replDetail.getInWareStock() != null && replDetail.getInWareStock() != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getInWareStock());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//前天销量
				if(replDetail.getSaleBeforeYesterdayQty() != null && replDetail.getSaleBeforeYesterdayQty() != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleBeforeYesterdayQty());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//昨天销量
				if(replDetail.getSaleYesterdayQty() != null && replDetail.getSaleYesterdayQty() != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleYesterdayQty());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//当天周转天数
				if(replDetail.getTurnoverDays() != null && replDetail.getTurnoverDays() != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getTurnoverDays());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//前14-前8天销量
				if(replDetail.getSaleBeforeSevenQty() != null && replDetail.getSaleBeforeSevenQty() != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleBeforeSevenQty());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//前7天销量
				if(replDetail.getSaleSevenQty() != null && replDetail.getSaleSevenQty() != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleSevenQty());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//销售变化 (增长、下降、不变)
				rowIndex.createCell(index++).setCellValue(replDetail.getSalesChanges());
				
				//晓望当天总营业额 (USD)
				if(replDetail.getSaleBeforeYesterdayAmount() != null 
						&& replDetail.getSaleBeforeYesterdayAmount().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleBeforeYesterdayAmount().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//晓望当天总成本 (USD)
				if(replDetail.getSaleBeforeYesterdayCost() != null 
						&& replDetail.getSaleBeforeYesterdayCost().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleBeforeYesterdayCost().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				
				//晓望7000运费总利润 (USD)
				if(replDetail.getDailyProfitAmont() != null 
						&& replDetail.getDailyProfitAmont().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getDailyProfitAmont().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//晓望7000运费利润率
				if(replDetail.getDailyProfitAvg() != null 
						&& replDetail.getDailyProfitAvg().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getDailyProfitAvg() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//定价利润 (盈利/亏损)(晓望7000)
				rowIndex.createCell(index++).setCellValue(replDetail.getDailyPricingProfit());
				
				//晓望当天总利润 (USD)
				if(replDetail.getSaleBeforeYesterdayProfit() != null 
						&& replDetail.getSaleBeforeYesterdayProfit().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleBeforeYesterdayProfit().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//晓望到箱利润率
				if(replDetail.getSaleProfitAvg() != null 
						&& replDetail.getSaleProfitAvg().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getSaleProfitAvg() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				
				//定价利润 (盈利/亏损)
				rowIndex.createCell(index++).setCellValue(replDetail.getPricingProfit());
				
				
				//Wayfair 零售价(Wayfair US 零售价)
				rowIndex.createCell(index++).setCellValue("");
				//链接首图
				rowIndex.createCell(index++).setCellValue("");
				//零售价更新日期
				rowIndex.createCell(index++).setCellValue("");
				//Wayfair US利润率
				if(replDetail.getWayfairUSProfitMargin() != null 
						&& replDetail.getWayfairUSProfitMargin().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getWayfairUSProfitMargin() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Wayfair 竞品零售价(Wayfair CA零售价)
				rowIndex.createCell(index++).setCellValue("");
				//竞品首图
				rowIndex.createCell(index++).setCellValue("");
				
				//Wayfair CA零售价
				rowIndex.createCell(index++).setCellValue("");
				//链接首图
				rowIndex.createCell(index++).setCellValue("");
				//零售价更新日期
				rowIndex.createCell(index++).setCellValue("");
				//Wayfair CA利润率
				if(replDetail.getWayfairCAProfitMargin() != null 
						&& replDetail.getWayfairCAProfitMargin().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getWayfairCAProfitMargin() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Wayfair 竞品零售价
				rowIndex.createCell(index++).setCellValue("");
				//竞品首图
				rowIndex.createCell(index++).setCellValue("");
				
				//Amazon 零售价(Amazon CA零售价)
				rowIndex.createCell(index++).setCellValue("");
				//链接首图
				rowIndex.createCell(index++).setCellValue("");
				//零售价更新日期
				rowIndex.createCell(index++).setCellValue("");
				//Amazon CA利润率
				if(replDetail.getAmazonCAProfitMargin() != null 
						&& replDetail.getAmazonCAProfitMargin().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getAmazonCAProfitMargin() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Amazon 竞品零售价
				rowIndex.createCell(index++).setCellValue("");
				//竞品首图
				rowIndex.createCell(index++).setCellValue("");
				
				//Walmart 零售价(Walmart US零售价)
				rowIndex.createCell(index++).setCellValue("");
				//链接首图
				rowIndex.createCell(index++).setCellValue("");
				//零售价更新日期
				rowIndex.createCell(index++).setCellValue("");
				//Walmart US利润率
				if(replDetail.getWalmartUSProfitMargin() != null 
						&& replDetail.getWalmartUSProfitMargin().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getWalmartUSProfitMargin() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Walmart 竞品零售价
				rowIndex.createCell(index++).setCellValue("");
				//竞品首图
				rowIndex.createCell(index++).setCellValue("");
				
				//Walmart CA零售价
				rowIndex.createCell(index++).setCellValue("");
				//链接首图
				rowIndex.createCell(index++).setCellValue("");
				//零售价更新日期
				rowIndex.createCell(index++).setCellValue("");
				//Walmart CA利润率
				if(replDetail.getWalmartCAProfitMargin() != null 
						&& replDetail.getWalmartCAProfitMargin().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getWalmartCAProfitMargin() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Walmart 竞品零售价
				rowIndex.createCell(index++).setCellValue("");
				//竞品首图
				rowIndex.createCell(index++).setCellValue("");
				
				//The Home Depot 零售价
				rowIndex.createCell(index++).setCellValue("");
				//链接首图
				rowIndex.createCell(index++).setCellValue("");
				//零售价更新日期
				rowIndex.createCell(index++).setCellValue("");
				//The Home Depot 利润率
				if(replDetail.getHomedepotProfitMargin() != null 
						&& replDetail.getHomedepotProfitMargin().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getHomedepotProfitMargin() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//The Home Depot 竞品零售价
				rowIndex.createCell(index++).setCellValue("");
				//竞品首图
				rowIndex.createCell(index++).setCellValue("");
				
				//Overstock 零售价
				rowIndex.createCell(index++).setCellValue("");
				//链接首图
				rowIndex.createCell(index++).setCellValue("");
				//零售价更新日期
				rowIndex.createCell(index++).setCellValue("");
				//Overstock 利润率
				if(replDetail.getOverstockProfitMargin() != null 
						&& replDetail.getOverstockProfitMargin().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(replDetail.getOverstockProfitMargin() + " %");
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Overstock竞品零售价
				rowIndex.createCell(index++).setCellValue("");
				//竞品首图
				rowIndex.createCell(index++).setCellValue("");
				
				//四字机构
				rowIndex.createCell(index++).setCellValue(replDetail.getFourUnit());
				//国家
				rowIndex.createCell(index++).setCellValue(replDetail.getCountryCode());
				
				count++;
			}
		}
		return workbook;
	}
	

	private List<FourUnitReplXW> getXWSumReplData(List<FourUnitMerchantRepl> fourUnitReplList, Integer sumType,
			Map<String, FourUnitSales> salesByKeyMap, Map<String, Map<String, FourUnitSales>> platResultMap,
			Map<String, KaSkuSeriesMapping> skuSeriesMappingByNameMap) {
		// TODO 过滤，汇总数据
		//过滤，汇总
        Map<String, FourUnitReplXW> replBySumKeyMap = new HashMap<>();
        FourUnitReplXW fourUnitReplXW = new FourUnitReplXW();
        
        String pricingProfit = "";
        String salesChanges = "";
        BigDecimal saleYesterdayQty = BigDecimal.ZERO;
        BigDecimal saleBeforeYesterdayAmount = BigDecimal.ZERO;
        BigDecimal saleBeforeYesterdayCost = BigDecimal.ZERO;
        BigDecimal saleBeforeYesterdayProfit = BigDecimal.ZERO;
        for (FourUnitMerchantRepl replDetail : fourUnitReplList) {
        	
        	//20230327 2、导出模板中过滤掉Sub-Category取值为spare part的数据
        	String category = replDetail.getCategory() == null ? "" : replDetail.getCategory();
			if(category.equalsIgnoreCase("Spare Parts")){
				continue;
			}
        	
        	String meterial = replDetail.getMeterial();
        	String succession = "";//系列
        	if(skuSeriesMappingByNameMap.containsKey(meterial)) {
        		succession = skuSeriesMappingByNameMap.get(meterial).getSuccession();
        	}
        	BigDecimal salesTwoWeeks = NumberUtil.getBigDecimal(replDetail.getSalesTwoWeeks());//前2周销售
        	BigDecimal salesSevenDays = NumberUtil.getBigDecimal(replDetail.getSalesSevenDays());//前7天销售
        	BigDecimal salesYesterday = NumberUtil.getBigDecimal(replDetail.getSalesYesterday());//前日销售
        	//前14-前8天销量
        	BigDecimal saleBeforeSevenQty = salesTwoWeeks.subtract(salesSevenDays);
        	//海外总库存(含锁定)
        	BigDecimal inventorySaleableOs = NumberUtil.getBigDecimal(replDetail.getInventorySaleableOs());
        	if(salesTwoWeeks.compareTo(BigDecimal.ZERO) <= 0 && inventorySaleableOs.compareTo(BigDecimal.ZERO) <= 0) {
        		//销量和库存都没有的数据，不统计
        		continue;
        	}
			
			//系列 + SKU+ 四字机构 + 国家
			String sumKey = succession + "," + replDetail.getSku() + "," + replDetail.getCompany() 
				+ "," + replDetail.getCountry();
			if(sumType.equals(2)) {
				//系列 + 类别 + 四字机构
				sumKey = succession + "," + replDetail.getCategory() + "," + replDetail.getCompany()
					+ "," + replDetail.getCountry();
			}
			if(replBySumKeyMap.containsKey(sumKey)) {
				fourUnitReplXW = replBySumKeyMap.get(sumKey);
				
				fourUnitReplXW.setInWareStock(
						inventorySaleableOs.add(NumberUtil.getBigDecimal(fourUnitReplXW.getInWareStock())).intValue());
				
				fourUnitReplXW.setSaleBeforeYesterdayQty(
						salesYesterday.add(NumberUtil.getBigDecimal(fourUnitReplXW.getSaleBeforeYesterdayQty())).intValue());
				fourUnitReplXW.setSaleYesterdayQty(
						saleYesterdayQty.add(NumberUtil.getBigDecimal(fourUnitReplXW.getSaleYesterdayQty())).intValue());
				fourUnitReplXW.setSaleBeforeSevenQty(
						saleBeforeSevenQty.add(NumberUtil.getBigDecimal(fourUnitReplXW.getSaleBeforeSevenQty())).intValue());
				fourUnitReplXW.setSaleSevenQty(
						salesSevenDays.add(NumberUtil.getBigDecimal(fourUnitReplXW.getSaleSevenQty())).intValue());
				fourUnitReplXW.setSaleBeforeYesterdayAmount(
						saleBeforeYesterdayAmount.add(NumberUtil.getBigDecimal(fourUnitReplXW.getSaleBeforeYesterdayAmount())));
				fourUnitReplXW.setSaleBeforeYesterdayCost(
						saleBeforeYesterdayCost.add(NumberUtil.getBigDecimal(fourUnitReplXW.getSaleBeforeYesterdayCost())));
				fourUnitReplXW.setSaleBeforeYesterdayProfit(
						saleBeforeYesterdayProfit.add(NumberUtil.getBigDecimal(fourUnitReplXW.getSaleBeforeYesterdayProfit())));
			} else {
				fourUnitReplXW = new FourUnitReplXW();
				fourUnitReplXW.setSuccession(succession);
				fourUnitReplXW.setSku(replDetail.getSku());
				fourUnitReplXW.setHibSkuName(replDetail.getMeterial());//英文名
				fourUnitReplXW.setCategory(replDetail.getCategory());
				fourUnitReplXW.setInWareStock(inventorySaleableOs.intValue());
				fourUnitReplXW.setSaleBeforeYesterdayQty(salesYesterday.intValue());
				fourUnitReplXW.setSaleYesterdayQty(saleYesterdayQty.intValue());
				fourUnitReplXW.setSaleBeforeSevenQty(saleBeforeSevenQty.intValue());
				fourUnitReplXW.setSaleSevenQty(salesSevenDays.intValue());
				//fourUnitReplXW.setSalesChanges(salesChanges);
				fourUnitReplXW.setPricingProfit(pricingProfit);
				fourUnitReplXW.setSaleBeforeYesterdayAmount(saleBeforeYesterdayAmount);
				fourUnitReplXW.setSaleBeforeYesterdayCost(saleBeforeYesterdayCost);
				fourUnitReplXW.setSaleBeforeYesterdayProfit(saleBeforeYesterdayProfit);
				fourUnitReplXW.setFourUnit(replDetail.getCompany());
				fourUnitReplXW.setCountryCode(replDetail.getCountry());
				
				replBySumKeyMap.put(sumKey, fourUnitReplXW);
			}
			
		}
        
        Set<String> haveKeySet = new HashSet<>();
        List<FourUnitReplXW> fourNewReplList = new ArrayList<>();//返回结果集
        if(replBySumKeyMap.size() > 0) {
        	for (Entry<String, FourUnitReplXW> replEntry : replBySumKeyMap.entrySet()) {
        		String key = replEntry.getKey();
        		
        		FourUnitReplXW unitReplXW = replEntry.getValue();
        		//前14-前8天销量
        		BigDecimal saleBeforeSevenQty = NumberUtil.getBigDecimal(unitReplXW.getSaleBeforeSevenQty());
        		//前7天销量
        		BigDecimal saleSevenQty = NumberUtil.getBigDecimal(unitReplXW.getSaleSevenQty());
        		//当天在仓库存量
        		BigDecimal inWareStock = NumberUtil.getBigDecimal(unitReplXW.getInWareStock());
        		
        		/**
            	 * 前7天销量 减 前14-前8天销量: >0 ，增长； =0,    不变； <0， 下降
            	 */
            	BigDecimal subtractSaleChange = saleSevenQty.subtract(saleBeforeSevenQty);
            	if(subtractSaleChange.compareTo(BigDecimal.ZERO) > 0) {
            		salesChanges = "增长";
            	} else if(subtractSaleChange.compareTo(BigDecimal.ZERO) < 0) {
            		salesChanges = "下降";
            	} else {
            		salesChanges = "不变";
            	}
            	unitReplXW.setSalesChanges(salesChanges);
            	
            	//公式计算，当天在仓库存量/ 当天日均销量
            	BigDecimal saleAvg = saleSevenQty.divide(new BigDecimal(7), 4, BigDecimal.ROUND_HALF_DOWN);
            	BigDecimal turnoverDays = BigDecimal.ZERO;
            	if(saleAvg.compareTo(BigDecimal.ZERO) != 0) {
            		turnoverDays = inWareStock.divide(BigDecimal.ONE, 0, BigDecimal.ROUND_HALF_DOWN);
            	}
            	unitReplXW.setTurnoverDays(turnoverDays.intValue());
            	
            	saleYesterdayQty = BigDecimal.ZERO;
                saleBeforeYesterdayAmount = BigDecimal.ZERO;
                saleBeforeYesterdayCost = BigDecimal.ZERO;
                saleBeforeYesterdayProfit = BigDecimal.ZERO;
            	//昨日销量
            	if(salesByKeyMap.containsKey(key)) {
        			FourUnitSales fourUnitSales = salesByKeyMap.get(key);
        			saleYesterdayQty = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayQty());
        			saleBeforeYesterdayAmount = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayAmount());
        			saleBeforeYesterdayCost = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayCost());
        			saleBeforeYesterdayProfit = saleBeforeYesterdayAmount.subtract(saleBeforeYesterdayCost);
        			haveKeySet.add(key);
        		}
            	unitReplXW.setSaleYesterdayQty(saleYesterdayQty.intValue());
            	unitReplXW.setSaleBeforeYesterdayAmount(saleBeforeYesterdayAmount);
            	unitReplXW.setSaleBeforeYesterdayCost(saleBeforeYesterdayCost);
            	unitReplXW.setSaleBeforeYesterdayProfit(saleBeforeYesterdayProfit);
            	
            	/** 20230331 获取平台对应的数据 */
            	unitReplXW = getPlatformProfit(platResultMap, key, unitReplXW);
            	
            	/**20230327
            	 * 定价利润 (盈利/亏损):利润>0，盈利；利润<0亏损
            	 */
            	pricingProfit = "";
            	if(saleBeforeYesterdayProfit.compareTo(BigDecimal.ZERO) > 0) {
            		pricingProfit = "盈利";
            	}
            	else if(saleBeforeYesterdayProfit.compareTo(BigDecimal.ZERO) < 0) {
            		pricingProfit = "亏损";
            	}
            	unitReplXW.setPricingProfit(pricingProfit);
            	/**
            	 * 20230327 晓望到箱利润率:利润/ 营业额， 百分比
            	 */
            	BigDecimal saleProfitAvg = BigDecimal.ZERO;
            	if(saleBeforeYesterdayAmount.compareTo(BigDecimal.ZERO) != 0) {
            		saleProfitAvg = saleBeforeYesterdayProfit.multiply(new BigDecimal(100))
            				.divide(saleBeforeYesterdayAmount, 2, BigDecimal.ROUND_HALF_UP);
            	}
            	unitReplXW.setSaleProfitAvg(saleProfitAvg);
            	
            	/** 20230331 取值和晓望到箱总利润一致 */
            	unitReplXW.setDailyProfitAmont(saleBeforeYesterdayProfit);
            	unitReplXW.setDailyProfitAvg(saleProfitAvg);
            	unitReplXW.setDailyPricingProfit(pricingProfit);
            	
        		fourNewReplList.add(unitReplXW);
			}
        	
        	//补充补货表没有的昨日销量纪录，预防SKU维度和款式维度数据不一致
        	for (Entry<String, FourUnitSales> salesEntry : salesByKeyMap.entrySet()) {
        		String saleKey = salesEntry.getKey();
        		FourUnitSales fourUnitSales = salesEntry.getValue();
        		
        		saleBeforeYesterdayAmount = BigDecimal.ZERO;
                saleBeforeYesterdayCost = BigDecimal.ZERO;
                saleBeforeYesterdayProfit = BigDecimal.ZERO;
        		if(!haveKeySet.contains(saleKey)) {
        			salesChanges = "不变";
        			String succession = "";//系列
                	if(skuSeriesMappingByNameMap.containsKey(fourUnitSales.getProductName())) {
                		succession = skuSeriesMappingByNameMap.get(fourUnitSales.getProductName()).getSuccession();
                	}
        			
        			saleBeforeYesterdayAmount = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayAmount());
        			saleBeforeYesterdayCost = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayCost());
        			saleBeforeYesterdayProfit = saleBeforeYesterdayAmount.subtract(saleBeforeYesterdayCost);
        			
        			FourUnitReplXW unitReplXW = new FourUnitReplXW();
        			/** 20230331 获取平台对应的数据 */
                	unitReplXW = getPlatformProfit(platResultMap, saleKey, unitReplXW);
                	
        			unitReplXW.setSuccession(succession);
        			unitReplXW.setSku(fourUnitSales.getProductNumber());
        			unitReplXW.setHibSkuName(fourUnitSales.getProductName());
        			unitReplXW.setCategory(fourUnitSales.getCategoryEnglishName());
        			unitReplXW.setInWareStock(0);
        			unitReplXW.setSaleBeforeYesterdayQty(0);
        			unitReplXW.setSaleYesterdayQty(fourUnitSales.getSaleYesterdayQty());
        			unitReplXW.setSaleBeforeSevenQty(0);
        			unitReplXW.setSaleSevenQty(0);
        			unitReplXW.setSalesChanges(salesChanges);
    				unitReplXW.setSaleBeforeYesterdayAmount(saleBeforeYesterdayAmount);
    				unitReplXW.setSaleBeforeYesterdayCost(saleBeforeYesterdayCost);
    				unitReplXW.setSaleBeforeYesterdayProfit(saleBeforeYesterdayProfit);
    				unitReplXW.setFourUnit(fourUnitSales.getFourUnit());
    				unitReplXW.setCountryCode(fourUnitSales.getSiteName());
    				
    				/**20230327
                	 * 定价利润 (盈利/亏损):利润>0，盈利；利润<0亏损
                	 */
                	pricingProfit = "";
                	if(saleBeforeYesterdayProfit.compareTo(BigDecimal.ZERO) > 0) {
                		pricingProfit = "盈利";
                	}
                	else if(saleBeforeYesterdayProfit.compareTo(BigDecimal.ZERO) < 0) {
                		pricingProfit = "亏损";
                	}
                	unitReplXW.setPricingProfit(pricingProfit);
                	/**
                	 * 20230327 晓望到箱利润率:利润/ 营业额， 百分比
                	 */
                	BigDecimal saleProfitAvg = BigDecimal.ZERO;
                	if(saleBeforeYesterdayAmount.compareTo(BigDecimal.ZERO) != 0) {
                		saleProfitAvg = saleBeforeYesterdayProfit.multiply(new BigDecimal(100))
                				.divide(saleBeforeYesterdayAmount, 2, BigDecimal.ROUND_HALF_UP);
                	}
                	unitReplXW.setSaleProfitAvg(saleProfitAvg);
    				
    				/** 20230331 取值和晓望到箱总利润一致 */
                	unitReplXW.setDailyProfitAmont(saleBeforeYesterdayProfit);
                	unitReplXW.setDailyProfitAvg(saleProfitAvg);
                	unitReplXW.setDailyPricingProfit(pricingProfit);
    				
    				fourNewReplList.add(unitReplXW);
        		}
			}
        	
        	
        }
        
        return fourNewReplList;
	}
	
	private FourUnitReplXW getPlatformProfit(Map<String, Map<String, FourUnitSales>> platResultMap,
			String sumKey, FourUnitReplXW unitReplXW) {
		/** 20230331 获取平台对应的数据 */
    	BigDecimal overstockProfit = BigDecimal.ZERO;
    	BigDecimal wayfairUSProfit = BigDecimal.ZERO;
    	BigDecimal wayfairCAProfit = BigDecimal.ZERO;
    	BigDecimal walmartUSProfit = BigDecimal.ZERO;
    	BigDecimal walmartCAProfit = BigDecimal.ZERO;
    	BigDecimal homedepotProfit = BigDecimal.ZERO;
    	BigDecimal amazonCAProfit = BigDecimal.ZERO;
    	
    	//平台收入
    	BigDecimal overstockIncome = BigDecimal.ZERO;
    	BigDecimal wayfairUSIncome = BigDecimal.ZERO;
    	BigDecimal wayfairCAIncome = BigDecimal.ZERO;
    	BigDecimal walmartUSIncome = BigDecimal.ZERO;
    	BigDecimal walmartCAIncome = BigDecimal.ZERO;
    	BigDecimal homedepotIncome = BigDecimal.ZERO;
    	BigDecimal amazonCAIncome = BigDecimal.ZERO;
    	if(platResultMap.containsKey(sumKey)) {
    		Map<String, FourUnitSales> map = platResultMap.get(sumKey);
    		FourUnitSales overstockSales = map.get("OVERSTOCK");
    		overstockProfit = getPlatformProfit(overstockSales);
    		overstockIncome = getPlatformIncome(overstockSales);
    		
    		FourUnitSales wayfairUSSales = map.get("WAYFAIR" + "," + "US");
    		wayfairUSProfit = getPlatformProfit(wayfairUSSales);
    		wayfairUSIncome = getPlatformIncome(wayfairUSSales);
    		
    		FourUnitSales wayfairCASales = map.get("WAYFAIR" + "," + "CA");
    		wayfairCAProfit = getPlatformProfit(wayfairCASales);
    		wayfairCAIncome = getPlatformIncome(wayfairCASales);
    		
    		FourUnitSales walmartUSSales = map.get("WALMART" + "," + "US");
    		walmartUSProfit = getPlatformProfit(walmartUSSales);
    		walmartUSIncome = getPlatformIncome(walmartUSSales);
    		
    		FourUnitSales walmartCASales = map.get("WALMART" + "," + "CA");
    		walmartCAProfit = getPlatformProfit(walmartCASales);
    		walmartCAIncome = getPlatformIncome(walmartCASales);
    		
    		FourUnitSales homedepotSales = map.get("HOME_DEPOT");
    		homedepotProfit = getPlatformProfit(homedepotSales);
    		homedepotIncome = getPlatformIncome(homedepotSales);
    		
    		FourUnitSales amazonCASales = map.get("AMAZON" + "," + "CA");
    		amazonCAProfit = getPlatformProfit(amazonCASales);
    		amazonCAIncome = getPlatformIncome(amazonCASales);
    	}
    	
    	//例如：【Wayfair US利润率】 = （【Wayfair US利润】 - 【Wayfair US当天总成本】）/【Wayfair US当天总营业额】
    	BigDecimal overstockProfitMargin = BigDecimal.ZERO;
    	BigDecimal wayfairUSProfitMargin = BigDecimal.ZERO;
    	BigDecimal wayfairCAProfitMargin = BigDecimal.ZERO;
    	BigDecimal walmartUSProfitMargin = BigDecimal.ZERO;
    	BigDecimal walmartCAProfitMargin = BigDecimal.ZERO;
    	BigDecimal homedepotProfitMargin = BigDecimal.ZERO;
    	BigDecimal amazonCAProfitMargin = BigDecimal.ZERO;
    	if(overstockIncome.compareTo(BigDecimal.ZERO) != 0) {
    		overstockProfitMargin = overstockProfit.multiply(new BigDecimal(100)).divide(overstockIncome, 2, BigDecimal.ROUND_HALF_UP);
    	}
    	if(wayfairUSIncome.compareTo(BigDecimal.ZERO) != 0) {
    		wayfairUSProfitMargin = wayfairUSProfit.multiply(new BigDecimal(100)).divide(wayfairUSIncome, 2, BigDecimal.ROUND_HALF_UP);
    	}
    	if(wayfairCAIncome.compareTo(BigDecimal.ZERO) != 0) {
    		wayfairCAProfitMargin = wayfairCAProfit.multiply(new BigDecimal(100)).divide(wayfairCAIncome, 2, BigDecimal.ROUND_HALF_UP);
    	}
    	if(walmartUSIncome.compareTo(BigDecimal.ZERO) != 0) {
    		walmartUSProfitMargin = walmartUSProfit.multiply(new BigDecimal(100)).divide(walmartUSIncome, 2, BigDecimal.ROUND_HALF_UP);
    	}
    	if(walmartCAIncome.compareTo(BigDecimal.ZERO) != 0) {
    		walmartCAProfitMargin = walmartCAProfit.multiply(new BigDecimal(100)).divide(walmartCAIncome, 2, BigDecimal.ROUND_HALF_UP);
    	}
    	if(homedepotIncome.compareTo(BigDecimal.ZERO) != 0) {
    		homedepotProfitMargin = homedepotProfit.multiply(new BigDecimal(100)).divide(homedepotIncome, 2, BigDecimal.ROUND_HALF_UP);
    	}
    	if(amazonCAIncome.compareTo(BigDecimal.ZERO) != 0) {
    		amazonCAProfitMargin = amazonCAProfit.multiply(new BigDecimal(100)).divide(amazonCAIncome, 2, BigDecimal.ROUND_HALF_UP);
    	}
    	
    	
    	unitReplXW.setWayfairUSProfit(wayfairUSProfit);
    	unitReplXW.setWayfairUSProfitMargin(wayfairUSProfitMargin);
    	
    	unitReplXW.setWayfairCAProfit(wayfairCAProfit);
    	unitReplXW.setWayfairCAProfitMargin(wayfairCAProfitMargin);
    	
    	unitReplXW.setAmazonCAProfit(amazonCAProfit);
    	unitReplXW.setAmazonCAProfitMargin(amazonCAProfitMargin);
    	
    	unitReplXW.setWalmartUSProfit(walmartUSProfit);
    	unitReplXW.setWalmartUSProfitMargin(walmartUSProfitMargin);
    	
    	unitReplXW.setWalmartCAProfit(walmartCAProfit);
    	unitReplXW.setWalmartCAProfitMargin(walmartCAProfitMargin);
    	
    	unitReplXW.setHomedepotProfit(homedepotProfit);
    	unitReplXW.setHomedepotProfitMargin(homedepotProfitMargin);
    	
    	unitReplXW.setOverstockProfit(overstockProfit);
    	unitReplXW.setOverstockProfitMargin(overstockProfitMargin);
    	
    	return unitReplXW;
	}
	
	private BigDecimal getPlatformProfit(FourUnitSales fourUnitSales) {
		if(fourUnitSales == null) {
			return BigDecimal.ZERO;
		}
		BigDecimal saleBeforeYesterdayAmount = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayAmount());
		BigDecimal saleBeforeYesterdayCost = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayCost());
		BigDecimal saleBeforeYesterdayProfit = saleBeforeYesterdayAmount.subtract(saleBeforeYesterdayCost);
		return saleBeforeYesterdayProfit;
	}
	
	private BigDecimal getPlatformIncome(FourUnitSales fourUnitSales) {
		if(fourUnitSales == null) {
			return BigDecimal.ZERO;
		}
		BigDecimal saleBeforeYesterdayAmount = NumberUtil.getBigDecimal(fourUnitSales.getSaleYesterdayAmount());
		return saleBeforeYesterdayAmount;
	}
	
	
	private List<String> getUserColonyFourUnitList(FourOrganizationDto fourOrganization){
		
		List<String> fourUnitList = new ArrayList<>();
		/** 20230505 根据用户账号查询集群下的四字机构 */
		
        FourOrganizationDto fourOrganDto = fourOrganizationApi.selectByPrimaryKey(fourOrganization.getAid());
        if(fourOrganDto != null) {
        	Integer alevel = fourOrganDto.getAlevel();
        	if(alevel.equals(1)) {//集群
        		FourOrganizationQueryDto fourOrganizationQueryDto = new FourOrganizationQueryDto();
        		fourOrganizationQueryDto.setAparentid(fourOrganDto.getAid());
        		List<FourOrganizationDto> colonyFourList = fourOrganizationApi.selectByQuery(fourOrganizationQueryDto);
        		if(colonyFourList != null && colonyFourList.size() > 0) {
        			for (FourOrganizationDto fourDto : colonyFourList) {
        				Integer alevel2 = fourDto.getAlevel();
        				if(alevel2.equals(2)) {
        					fourUnitList.add(fourDto.getAname());
        				}
        			}
        		}
        	}else if(alevel.equals(2)) {//四字机构
        		FourOrganizationQueryDto fourOrganizationQueryDto = new FourOrganizationQueryDto();
        		fourOrganizationQueryDto.setAparentid(fourOrganDto.getAparentid());
        		List<FourOrganizationDto> colonyFourList = fourOrganizationApi.selectByQuery(fourOrganizationQueryDto);
        		if(colonyFourList != null && colonyFourList.size() > 0) {
        			for (FourOrganizationDto fourDto : colonyFourList) {
        				Integer alevel2 = fourDto.getAlevel();
        				if(alevel2.equals(2)) {
        					fourUnitList.add(fourDto.getAname());
        				}
        			}
        		}
        	}else if(alevel.equals(3)) {//三字机构
        		FourOrganizationQueryDto fourOrganizationQueryDto = new FourOrganizationQueryDto();
        		fourOrganizationQueryDto.setAparentid(fourOrganDto.getAparentid());
        		List<FourOrganizationDto> fourList = fourOrganizationApi.selectByQuery(fourOrganizationQueryDto);
        		if(fourList != null && fourList.size() > 0) {
        			Integer colonyId = fourList.get(0).getAparentid();
        			if(colonyId != null) {//集群下的四字机构
	        			fourOrganizationQueryDto = new FourOrganizationQueryDto();
	        			fourOrganizationQueryDto.setAparentid(colonyId);
	        			List<FourOrganizationDto> colonyFourList = fourOrganizationApi.selectByQuery(fourOrganizationQueryDto);
	            		if(colonyFourList != null && colonyFourList.size() > 0) {
	            			for (FourOrganizationDto fourDto : colonyFourList) {
	            				Integer alevel2 = fourDto.getAlevel();
	            				if(alevel2.equals(2)) {
	            					fourUnitList.add(fourDto.getAname());
	            				}
	            			}
	            		}
        			}
        		}
        	}
        }
        
        return fourUnitList;
	}
	
		
	private List<String> getUserFourUnitList(FourOrganizationDto fourOrganization){
		List<String> fourUnitList = new ArrayList<>();
		/** 20230505 根据用户账号查询四字机构 */
		
        FourOrganizationDto fourOrganDto = fourOrganizationApi.selectByPrimaryKey(fourOrganization.getAid());
        if(fourOrganDto != null) {
        	Integer alevel = fourOrganDto.getAlevel();
        	if(alevel.equals(1)) {//集群
        		FourOrganizationQueryDto fourOrganizationQueryDto = new FourOrganizationQueryDto();
        		fourOrganizationQueryDto.setAparentid(fourOrganDto.getAid());
        		List<FourOrganizationDto> colonyFourList = fourOrganizationApi.selectByQuery(fourOrganizationQueryDto);
        		if(colonyFourList != null && colonyFourList.size() > 0) {
        			for (FourOrganizationDto fourDto : colonyFourList) {
        				Integer alevel2 = fourDto.getAlevel();
        				if(alevel2.equals(2)) {
        					fourUnitList.add(fourDto.getAname());
        				}
        			}
        		}
        	}else if(alevel.equals(2)) {//四字机构
        		fourUnitList.add(fourOrganization.getAname());
        	}else if(alevel.equals(3)) {//三字机构
        		FourOrganizationQueryDto fourOrganizationQueryDto = new FourOrganizationQueryDto();
        		fourOrganizationQueryDto.setAparentid(fourOrganDto.getAparentid());
        		List<FourOrganizationDto> fourList = fourOrganizationApi.selectByQuery(fourOrganizationQueryDto);
        		if(fourList != null && fourList.size() > 0) {
        			fourUnitList.add(fourList.get(0).getAname());
        		}
        	}
        }
        
        return fourUnitList;
	}
	
	

	@Override
	public void downloadEcommerceMonitor(Context context, HttpServletResponse response) {
		// TODO 20230423 lillian 电商监控表
		
		//账号信息：account.getAname() + "_" + account.getThirdPlatform().getAname()
		Map<String, ThirdAccountDto> thirdAccountReturnMap = thirdAccountApi.selectByQueryReturnMap(new ThirdAccountQueryDto());
		//查询入库商家
		Map<String, OrganizationDto> organizationReturnMap = organizationApi.selectByQueryReturnMap(new OrganizationQueryDto());
		
		//查询
		List<ErpCloudMaterialAllDto> coreAndSubArticleList = erpCloudMaterialNumApi.selectAllByCoreAndSubArticle();
		//ERP-云库存关联货号
        Map<String, ErpCloudMaterialAllDto> coreAndSubArticleSkuMap = 
        		Maps.uniqueIndex(coreAndSubArticleList, ErpCloudMaterialAllDto::getSubArticleSku);
        logger.info("coreAndSubArticleSkuMap size : {}", coreAndSubArticleSkuMap.size());
        //HIB-四字机构关联货号
        List<OrganizationRelativeMerchandiseDto> hibORMaterialList = organizationRelativeMerchandiseApi.selectAll();
        Map<String, OrganizationRelativeMerchandiseDto> hibFourUnitAssociatedMaterialMap = new HashMap<>();
        for (OrganizationRelativeMerchandiseDto allDto : hibORMaterialList) {
        	hibFourUnitAssociatedMaterialMap.put(allDto.getArelativeMerchandise().getAnumber(), allDto);
        }
        logger.info("hibFourUnitAssociatedMaterialMap size : {}", hibFourUnitAssociatedMaterialMap.size());
		
		//参数
		KaReplChartQuery replQuery = new KaReplChartQuery();
		
		List<String> fourUnitList = new ArrayList<>();
		//晓望
		/*fourUnitList.add("太华天街");
		fourUnitList.add("晓望梅观");
		fourUnitList.add("龙门客栈");*/
		//古月
		/*fourUnitList.add("水墨江南");
		fourUnitList.add("奇诺山庄");
		fourUnitList.add("渔人码头");
		fourUnitList.add("和平饭店");*/
		
		/** 20230505 根据用户账号查询集群下的四字机构 */
		Integer userId = Integer.valueOf((String) context.getContext(CacheDomain.CURRENT_USER_ID));
		UserDto userDto = userApi.selectByPrimaryKey(userId);
        if (userDto == null) {
        	response.setHeader("flag", "0");
            response.setHeader("msg", "用户不存在");
            return;
        }
        FourOrganizationDto fourOrganization = userDto.getFourOrganization();
        if (fourOrganization == null || fourOrganization.getAid() == null) {
        	response.setHeader("flag", "0");
            response.setHeader("msg", "用户没有维护所属机构");
            return;
        }
        //四字机构集合
        fourUnitList = getUserColonyFourUnitList(fourOrganization);
        
        if(fourUnitList == null || fourUnitList.size() == 0) {
        	response.setHeader("flag", "0");
            response.setHeader("msg", "用户没有对应集群信息");
            return;
        }
		
		replQuery.setFourUnitList(fourUnitList);
		replQuery.setAdate(new Date());//取最新的补货表数据
		
		//补货表-站点销售明细数据
		List<KaReplSiteSaledetail> replSiteSaleList = getReplSiteSaleData(replQuery);
		
		//补货表-入库商家明细
		List<FourUnitMerchantRepl> replMerchantStockList = getReplMerchantStockData(replQuery);
		Map<String, KaPlatformStockForHIB> selfStockMap = dealReplMerchantStockMap(replMerchantStockList);
		
		//dataaccess-平台库存
		KaStockForHIBResult kaStockForHIBResult =  getPlatformStockData(replQuery);
		List<KaPlatformStockForHIB> platformStockList = kaStockForHIBResult.getResultStockList();
		logger.info("platformStockList size : {}", platformStockList == null ? 0 : platformStockList.size());
		Map<String, List<KaPlatformStockForHIB>> platStockByKeyMap = dealPlatformStockData(platformStockList, fourUnitList,
				coreAndSubArticleSkuMap, hibFourUnitAssociatedMaterialMap);
		//入库商家对应账号信息
		//<入库商家,<站点,账号>>
		Map<String, Map<String, List<BasicPlatAccUnitCompany>>> merchantAccountMap = kaStockForHIBResult.getMerchantAccountMap();
		
		//查询SKU系列映射数据
		Map<String, KaSkuSeriesMapping> skuSeriesMappingByNameMap = getSkuSeriesMappingData();
				
		//获取昨日订单
		List<FourUnitSales> allOrderStockList = getOrderDetail(replQuery);
		Map<String, List<FourUnitSales>> orderByKeyMap = dealReplYesterdayOrder(allOrderStockList, fourUnitList, 
				skuSeriesMappingByNameMap, coreAndSubArticleSkuMap, hibFourUnitAssociatedMaterialMap);
		
		//订单大表快递费
		List<FourUnitSales> allOrderExpressFeeList = getOrderExpressFee(replQuery);
		Map<String, List<FourUnitSales>> orderExpressFeeByKeyMap = dealOrderExpressFee(allOrderExpressFeeList, fourUnitList, 
				coreAndSubArticleSkuMap, hibFourUnitAssociatedMaterialMap);
		
		
		/** 处理汇总数据 */
		List<EcommerceMonitor> ecommerceMonitorList = dealEcommerceResult(replSiteSaleList, selfStockMap, platStockByKeyMap, 
				orderByKeyMap, orderExpressFeeByKeyMap, thirdAccountReturnMap, skuSeriesMappingByNameMap, organizationReturnMap,
				merchantAccountMap);
		
		/** 查询物料图 */
		Set<String> skuNumberSet = ecommerceMonitorList.stream()
				.map(r -> Optional.ofNullable(r.getSkuNumber()).orElse("")).collect(Collectors.toSet());
		/*Map<String, MerchandiseDto> skuNOReturnMap = merchandiseApi.selectByAnumberListReturnMap(new ArrayList<>(skuNumberSet));*/
		Map<String, Map<String, MerchandiseImageDto>> imgMap = new HashMap<>();
//		if(skuNumberSet != null && skuNumberSet.size() > 0) {
//			imgMap =  merchandiseImageApi.selectIfMainByMerchandiseAnumberListToMap(new ArrayList<>(skuNumberSet));
//		}
		
		//创建excel
		OutputStream os = null;
		String pathName = "电商监控表.xlsx";//
		SXSSFWorkbook workbook = new SXSSFWorkbook(500);
		creatEcommerceMonitorExcel(ecommerceMonitorList, workbook, imgMap);
		
		try {
            response.reset();
            response.setContentType("application/vnd.ms-excel;charset=UTF-8");
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Disposition", "inline; filename=" + pathName);
            response.setHeader("fname", pathName);
            response.setHeader("flag", "1");
            os = response.getOutputStream();
            workbook.write(os);
            os.flush();
        } catch (Exception e) {
            logger.info("exportExcel", e);
            response.setHeader("flag", "0");
            response.setHeader("msg", e.getMessage());
        } finally {
            try {
                if (os != null)
                    os.close();
                workbook.close();
            } catch (IOException e) {
                logger.info("exportExcel close os or wb", e);
            }
        }
		logger.info("电商监控表 Download finish!");
	}
	
	
	private List<EcommerceMonitor> dealEcommerceResult(List<KaReplSiteSaledetail> replSiteSaleList,
			Map<String, KaPlatformStockForHIB> selfStockMap,
			Map<String, List<KaPlatformStockForHIB>> platStockByKeyMap,
			Map<String, List<FourUnitSales>> orderByKeyMap, Map<String, List<FourUnitSales>> orderExpressFeeByKeyMap,
			Map<String, ThirdAccountDto> thirdAccountReturnMap, Map<String, KaSkuSeriesMapping> skuSeriesMappingByNameMap,
			Map<String, OrganizationDto> organizationReturnMap,
			Map<String, Map<String, List<BasicPlatAccUnitCompany>>> merchantAccountMap) {
		
		List<EcommerceMonitor> ecommerceMonitorList = new ArrayList<>();
		
		//39Finc
		List<String> first39FincList = new ArrayList<>();
		first39FincList.add("39Finc");
		first39FincList.add("39F INC");
		//海外广新
		List<String> secondHWGXList = new ArrayList<>();
		secondHWGXList.add("Guangxin");
		secondHWGXList.add("GX");
		secondHWGXList.add("电商广新");
		secondHWGXList.add("Guangdong Guangxin Jiaju youxian gongsi");
		secondHWGXList.add("电商-广新家具");
		
		Set<String> orderByKeySet = new HashSet<>();
		Set<String> platStockByKeySet = new HashSet<>();
		if(replSiteSaleList != null && replSiteSaleList.size() > 0) {
			//补货表的站点销售明细
			for (KaReplSiteSaledetail kaReplSiteSaledetail : replSiteSaleList) {
				
				//20230327 2、导出模板中过滤掉Sub-Category取值为spare part的数据
	        	String category = kaReplSiteSaledetail.getCategory() == null ? "" : kaReplSiteSaledetail.getCategory();
				if(category.equalsIgnoreCase("Spare Parts")){
					continue;
				}
				//String meterial = kaReplSiteSaledetail.getMeterial();
				String fourUnit = kaReplSiteSaledetail.getFourUnit();
				
				EcommerceMonitor ecommerceMonitor = new EcommerceMonitor();
				BeanUtils.copyProperties(kaReplSiteSaledetail, ecommerceMonitor);
				ecommerceMonitorList.add(ecommerceMonitor);
				
				ecommerceMonitor.setMerchantName(kaReplSiteSaledetail.getSeller());
				ecommerceMonitor.setSkuNumber(kaReplSiteSaledetail.getSku());
				ecommerceMonitor = buildEcommerceMonitor(ecommerceMonitor, 
						kaReplSiteSaledetail.getAccountName(), kaReplSiteSaledetail.getPlatformName(),
						thirdAccountReturnMap, organizationReturnMap,
						first39FincList, secondHWGXList);
				
				//日均销量
				BigDecimal salesDayAvr = BigDecimal.ZERO;
				BigDecimal salesSevenDays = NumberUtil.getBigDecimal(kaReplSiteSaledetail.getSalesSevenDays());
				if(salesSevenDays.compareTo(BigDecimal.ZERO) != 0) {
					salesDayAvr = salesSevenDays.divide(new BigDecimal(7), 4, BigDecimal.ROUND_HALF_UP);
				}
				ecommerceMonitor.setSalesDayAvr(salesDayAvr);
				
				//平台，账号，站点，SKU编码
				String saleSkukey = kaReplSiteSaledetail.getPlatformName() + "," + kaReplSiteSaledetail.getAccountName()
					+ "," + kaReplSiteSaledetail.getSiteName() + "," + kaReplSiteSaledetail.getSku();
				
				BigDecimal retailPrice = BigDecimal.ZERO;
				if(orderByKeyMap.containsKey(saleSkukey)) {
					List<FourUnitSales> list = orderByKeyMap.get(saleSkukey);
					//总营业额
					BigDecimal sumSaleYesterdayAmount = list.stream()
		                    .filter(e -> e.getSaleYesterdayAmount() != null)
		                    .map(FourUnitSales::getSaleYesterdayAmount)
		                    .reduce(BigDecimal.ZERO, BigDecimal::add);
					//总成本
					BigDecimal sumSaleYesterdayCost = list.stream()
		                    .filter(e -> e.getSaleYesterdayCost() != null)
		                    .map(FourUnitSales::getSaleYesterdayCost)
		                    .reduce(BigDecimal.ZERO, BigDecimal::add);
					
					//7000运费总利润 (USD)
					BigDecimal sumYestProfit = sumSaleYesterdayAmount.subtract(sumSaleYesterdayCost);
					BigDecimal saleYesterdayProfitRadio = BigDecimal.ZERO;
					if(sumSaleYesterdayAmount.compareTo(BigDecimal.ZERO) != 0) {
						saleYesterdayProfitRadio = sumYestProfit.divide(sumSaleYesterdayAmount, 4, BigDecimal.ROUND_HALF_UP);
					}
					ecommerceMonitor.setSaleYesterdayProfit(sumYestProfit);
					ecommerceMonitor.setSaleYesterdayProfitRadio(saleYesterdayProfitRadio);
					/*//零售价:最新的销售价格
					ecommerceMonitor.setRetailPrice(list.get(0).getPrice());*/
					
					retailPrice = list.get(0).getPrice();
					orderByKeySet.add(saleSkukey);
				}
				if(retailPrice.compareTo(BigDecimal.ZERO) == 0) {
					//销售金额（美元）（过去30天）
					BigDecimal salesAmountThirty = NumberUtil.getBigDecimal(kaReplSiteSaledetail.getSalesAmountThirty());
					//月销量(过去30天)
					BigDecimal salesThirtyDays = NumberUtil.getBigDecimal(kaReplSiteSaledetail.getSalesThirtyDays());
					if(salesThirtyDays.compareTo(BigDecimal.ZERO) != 0) {
						retailPrice = salesAmountThirty.divide(salesThirtyDays, 4, BigDecimal.ROUND_HALF_UP);
					}
				}
				//零售价:最新的销售价格
				ecommerceMonitor.setRetailPrice(retailPrice);
				
				/** 订单30天快递费 */
				if(orderExpressFeeByKeyMap.containsKey(saleSkukey)) {
					List<FourUnitSales> list = orderExpressFeeByKeyMap.get(saleSkukey);
					//总快递费美元
					BigDecimal sumExpressFeeDol = list.stream()
		                    .filter(e -> e.getExpressFeeDol() != null)
		                    .map(FourUnitSales::getExpressFeeDol)
		                    .reduce(BigDecimal.ZERO, BigDecimal::add);
					
					//快递费
					ecommerceMonitor.setExpressFee(sumExpressFeeDol);
				}
				
				BigDecimal vol = NumberUtil.getBigDecimal(kaReplSiteSaledetail.getVol());
				BigDecimal outboundFee = vol.multiply(new BigDecimal(59));//出库费 = 物料的体积*US$59
				
				BigDecimal outboundCost = NumberUtil.getBigDecimal(kaReplSiteSaledetail.getUploadCost());
				if(fourUnit.equals("水墨江南") || fourUnit.equals("渔人码头")) {
					if(kaReplSiteSaledetail.getUploadDDPCost() != null) {
						outboundCost = NumberUtil.getBigDecimal(kaReplSiteSaledetail.getUploadDDPCost());
					}
				}
				
				//利润率 = （零售价*提现率-出库成本）/零售价
				BigDecimal withdrawalRate = NumberUtil.getBigDecimal(ecommerceMonitor.getWithdrawalRate());
				BigDecimal profitMargin = BigDecimal.ZERO;
				if(retailPrice != null && retailPrice.compareTo(BigDecimal.ZERO) != 0) {
					BigDecimal profitFirst = retailPrice.multiply(withdrawalRate).subtract(outboundCost);
					profitMargin = profitFirst.divide(retailPrice, 4, BigDecimal.ROUND_HALF_UP);
				}
				
				/** 平台库存 */
				BigDecimal platStock = BigDecimal.ZERO;
				if(platStockByKeyMap.containsKey(saleSkukey)) {
					List<KaPlatformStockForHIB> list = platStockByKeyMap.get(saleSkukey);
					//总库存
					long sumSiteQty = list.stream().collect(
							Collectors.summarizingInt(KaPlatformStockForHIB::getQtySiteSku)).getSum();
					platStock = NumberUtil.getBigDecimal(sumSiteQty);
					
					platStockByKeySet.add(saleSkukey);
				}
				
				if(fourUnit.equals("水墨江南") || fourUnit.equals("渔人码头")) {
					if(kaReplSiteSaledetail.getUploadFOBCost() != null) {
						ecommerceMonitor.setFobCost(kaReplSiteSaledetail.getUploadFOBCost());
					}
					if(kaReplSiteSaledetail.getUploadDDPCost() != null) {
						ecommerceMonitor.setDdpCost(kaReplSiteSaledetail.getUploadDDPCost());
					}
				}else {
					//ecommerceMonitor.setFobCost(kaReplSiteSaledetail.getUploadCost());
					ecommerceMonitor.setDdpCost(kaReplSiteSaledetail.getUploadCost());
				}
				
				ecommerceMonitor.setPlatformAccountName(kaReplSiteSaledetail.getAccountName());
				ecommerceMonitor.setSkuName(kaReplSiteSaledetail.getMeterial());
				ecommerceMonitor.setSkuCategory(kaReplSiteSaledetail.getCategory());
				ecommerceMonitor.setOutboundFee(outboundFee);
				ecommerceMonitor.setProfitMargin(profitMargin);
				ecommerceMonitor.setFourUnit(fourUnit);
				ecommerceMonitor.setPlatformStock(platStock);
			}
		}
		
		//平台库存
		for (Entry<String, List<KaPlatformStockForHIB>> entryPlatStock : platStockByKeyMap.entrySet()) {
			String saleSkukey = entryPlatStock.getKey();
			if(platStockByKeySet.contains(saleSkukey)) {
				continue;
			}
			List<KaPlatformStockForHIB> list = entryPlatStock.getValue();
			KaPlatformStockForHIB stockForHIB = list.get(0);
			
			BigDecimal platStock = BigDecimal.ZERO;
			//总库存
			long sumSiteQty = list.stream().collect(
					Collectors.summarizingInt(KaPlatformStockForHIB::getQtySiteSku)).getSum();
			platStock = NumberUtil.getBigDecimal(sumSiteQty);
			
			/**  */
			EcommerceMonitor ecommerceMonitor = new EcommerceMonitor();
			ecommerceMonitorList.add(ecommerceMonitor);
			
			ecommerceMonitor.setSkuNumber(stockForHIB.getMaterialNumber());
			ecommerceMonitor.setMerchantName(stockForHIB.getMerchantName());
			ecommerceMonitor = buildEcommerceMonitor(ecommerceMonitor, 
					stockForHIB.getAccountName(), stockForHIB.getPlatformName(),
					thirdAccountReturnMap, organizationReturnMap,
					first39FincList, secondHWGXList);
			ecommerceMonitor.setPlatformStock(platStock);
			
			ecommerceMonitor.setPlatformName(stockForHIB.getPlatformName());
			ecommerceMonitor.setPlatformAccountName(stockForHIB.getAccountName());
			ecommerceMonitor.setSiteName(stockForHIB.getSiteName());
			ecommerceMonitor.setSkuName(stockForHIB.getMaterialEngname());
			ecommerceMonitor.setSkuCategory(stockForHIB.getCategoryEnglishName());
			ecommerceMonitor.setFourUnit(stockForHIB.getFourUnit());
		}
		
		/** 处理自营仓库存 */
		Map<String, BigDecimal> sellerCountMap = new HashMap<>();
		for (EcommerceMonitor eMonitor : ecommerceMonitorList) {
			String merchantName = eMonitor.getMerchantName() == null ? "" : eMonitor.getMerchantName();
			String siteName = eMonitor.getSiteName() == null ? "" : eMonitor.getSiteName();
			String skuNumber = eMonitor.getSkuNumber() == null ? "" : eMonitor.getSkuNumber();
			String countryCode = siteName;
			if (siteName != null && "US,CA,MX,CN".indexOf(siteName) != -1) {
				countryCode = siteName;
            } else {
            	countryCode = "EU";
            }
			
			//key:国家简称 + 入库商家 + SKU
        	String selfStockKey = countryCode + "," + merchantName + "," + skuNumber;
			if(sellerCountMap.containsKey(selfStockKey)) {
				sellerCountMap.put(selfStockKey, BigDecimal.ONE.add(sellerCountMap.get(selfStockKey)));
			}else {
				sellerCountMap.put(selfStockKey, BigDecimal.ONE);
			}
		}
		
		Set<String> selfStockKeySet = new HashSet<>();
		for (EcommerceMonitor eMonitor : ecommerceMonitorList) {
			String merchantName = eMonitor.getMerchantName() == null ? "" : eMonitor.getMerchantName();
			String siteName = eMonitor.getSiteName() == null ? "" : eMonitor.getSiteName();
			String skuNumber = eMonitor.getSkuNumber() == null ? "" : eMonitor.getSkuNumber();
			String countryCode = siteName;
			if (siteName != null && "US,CA,MX,CN".indexOf(siteName) != -1) {
				countryCode = siteName;
            } else {
            	countryCode = "EU";
            }
			
			//key:国家简称 + 入库商家 + SKU
        	String selfStockKey = countryCode + "," + merchantName + "," + skuNumber;
        	BigDecimal selfHaveCount = BigDecimal.ZERO;
        	BigDecimal qtySelfHaveAll = BigDecimal.ZERO;
			if(selfStockMap.containsKey(selfStockKey)) {
				selfHaveCount = selfHaveCount.add(BigDecimal.ONE);
				KaPlatformStockForHIB selfStockForHIB = selfStockMap.get(selfStockKey);
				BigDecimal selfCount = sellerCountMap.get(selfStockKey);
				if(selfCount == null) {
					selfCount = BigDecimal.ONE;
				}
				
				BigDecimal qtySelfAll = NumberUtil.getBigDecimal(selfStockForHIB.getQtyAll());
				BigDecimal qtySelfAllAvg = qtySelfAll.divide(selfCount, 0, BigDecimal.ROUND_DOWN);
				
				if(selfHaveCount.compareTo(selfCount) == 0) {
					//最后一次
					qtySelfAllAvg = qtySelfAll.subtract(qtySelfHaveAll);
				}else {
					qtySelfHaveAll = qtySelfHaveAll.add(qtySelfAllAvg);
				}
				eMonitor.setSelfStock(qtySelfAllAvg);
				
				selfStockKeySet.add(selfStockKey);
			}
		}
		
		//自营仓库存
		for (Entry<String, KaPlatformStockForHIB> selfStockEntry : selfStockMap.entrySet()) {
			String selfStockKey = selfStockEntry.getKey();
			KaPlatformStockForHIB selfStockForHIB = selfStockEntry.getValue();
			if(selfStockKeySet.contains(selfStockKey)) {
				continue;
			}
			BigDecimal qtySelfAll = NumberUtil.getBigDecimal(selfStockForHIB.getQtyAll());
			BigDecimal sizeQty = qtySelfAll;
			boolean merchantFlag = false;
			if(merchantAccountMap != null) {//<入库商家,<站点,账号>>
				if(merchantAccountMap.containsKey(selfStockForHIB.getMerchantName())) {
					//<站点,账号>
					Map<String, List<BasicPlatAccUnitCompany>> siteAccountMap = merchantAccountMap.get(selfStockForHIB.getMerchantName());
					List<BasicPlatAccUnitCompany> accountList = new ArrayList<>();
					if(siteAccountMap.containsKey(selfStockForHIB.getSiteName())) {
						accountList = siteAccountMap.get(selfStockForHIB.getSiteName());
					}else {
						//随机取值一个站点下的账号
						for (Entry<String, List<BasicPlatAccUnitCompany>> siteEntry : siteAccountMap.entrySet()) {
							accountList = siteEntry.getValue();
							break;
						}
					}
					
					BigDecimal size = NumberUtil.getBigDecimal(accountList.size());
					BigDecimal sizeHave = BigDecimal.ZERO; 
					BigDecimal qtySelfHaveAll = BigDecimal.ZERO; 
					if(size.compareTo(BigDecimal.ZERO) != 0) {
						//sizeQty = qtySelfAll.divide(size, 2, BigDecimal.ROUND_HALF_DOWN);
						sizeQty = qtySelfAll.divide(size, 0, BigDecimal.ROUND_DOWN);
					}
					
					for (BasicPlatAccUnitCompany basicAcc : accountList) {
						sizeHave = sizeHave.add(BigDecimal.ONE);
						EcommerceMonitor ecommerceMonitor = new EcommerceMonitor();
						ecommerceMonitorList.add(ecommerceMonitor);
						
						ecommerceMonitor.setSkuNumber(selfStockForHIB.getMaterialNumber());
						ecommerceMonitor.setMerchantName(selfStockForHIB.getMerchantName());
						if(sizeHave.compareTo(size) == 0) {
							//最后一次
							sizeQty = qtySelfAll.subtract(qtySelfHaveAll);
						}else {
							qtySelfHaveAll = qtySelfHaveAll.add(sizeQty);
						}
						ecommerceMonitor.setSelfStock(sizeQty);
						
						ecommerceMonitor.setPlatformName(basicAcc.getPlatformName());
						ecommerceMonitor.setPlatformAccountName(basicAcc.getAccountName());
						if(basicAcc.getSiteName() != null && basicAcc.getSiteName().equals("Default")) {
							ecommerceMonitor.setSiteName(selfStockForHIB.getSiteName());
						}else {
							ecommerceMonitor.setSiteName(basicAcc.getSiteName());
						}
						ecommerceMonitor.setSkuName(selfStockForHIB.getMaterialEngname());
						ecommerceMonitor.setSkuCategory(selfStockForHIB.getCategoryEnglishName());
						ecommerceMonitor.setFourUnit(selfStockForHIB.getFourUnit());
						ecommerceMonitor = buildEcommerceMonitor(ecommerceMonitor, 
								basicAcc.getAccountName(), basicAcc.getPlatformName(),
								thirdAccountReturnMap, organizationReturnMap,
								first39FincList, secondHWGXList);
					}
					//merchantFlag = true;
				}else {
					merchantFlag = true;
				}
			}else {
				merchantFlag = true;
			}
			if (merchantFlag) {
				/**  */
				EcommerceMonitor ecommerceMonitor = new EcommerceMonitor();
				ecommerceMonitorList.add(ecommerceMonitor);
				
				ecommerceMonitor.setSkuNumber(selfStockForHIB.getMaterialNumber());
				ecommerceMonitor.setMerchantName(selfStockForHIB.getMerchantName());
				ecommerceMonitor.setSelfStock(qtySelfAll);
				
				ecommerceMonitor.setSiteName(selfStockForHIB.getSiteName());
				ecommerceMonitor.setSkuName(selfStockForHIB.getMaterialEngname());
				ecommerceMonitor.setSkuCategory(selfStockForHIB.getCategoryEnglishName());
				ecommerceMonitor.setFourUnit(selfStockForHIB.getFourUnit());
				ecommerceMonitor = buildEcommerceMonitor(ecommerceMonitor, 
						"", "",
						thirdAccountReturnMap, organizationReturnMap,
						first39FincList, secondHWGXList);
			}
		}
		
		
		/** 计算结果集 */
		for (EcommerceMonitor eMonitor : ecommerceMonitorList) {
			BigDecimal platformStock = NumberUtil.getBigDecimal(eMonitor.getPlatformStock());
			BigDecimal selfStock = NumberUtil.getBigDecimal(eMonitor.getSelfStock());
			//库存量
			BigDecimal totalInventory = platformStock.add(selfStock);
			
			//周转天数 = 总库存量/销量
			BigDecimal turnoverDays = BigDecimal.ZERO;
			BigDecimal salesDayAvr = eMonitor.getSalesDayAvr();
			if(salesDayAvr != null && salesDayAvr.compareTo(BigDecimal.ZERO) != 0) {
				turnoverDays = totalInventory.divide(salesDayAvr, 0, BigDecimal.ROUND_UP);
			}
			
			String skuName = eMonitor.getSkuName();
			if(skuSeriesMappingByNameMap.containsKey(skuName)) {
				//匹配SKU系列映射
				KaSkuSeriesMapping kaSkuSeriesMapping = skuSeriesMappingByNameMap.get(skuName);
				eMonitor.setSuccession(kaSkuSeriesMapping.getSuccession());
			}
			
			eMonitor.setTotalInventory(totalInventory);
			eMonitor.setTurnoverDays(turnoverDays);
			
			if(eMonitor.getFinanceCompany() == null) {
				eMonitor = buildEcommerceMonitor(eMonitor, 
						eMonitor.getPlatformAccountName(), eMonitor.getPlatformName(),
						thirdAccountReturnMap, organizationReturnMap,
						first39FincList, secondHWGXList);
			}
		}
		
		return ecommerceMonitorList;
	}
	
	
	private EcommerceMonitor buildEcommerceMonitor(EcommerceMonitor ecommerceMonitor,
			String accountName, String platformName, 
			Map<String, ThirdAccountDto> thirdAccountReturnMap, Map<String, OrganizationDto> organizationReturnMap,
			List<String> first39FincList, List<String> secondHWGXList) {
		
		String organizationName = ecommerceMonitor.getMerchantName();
		
		String paymentrecoverycompany = "";
		String sellerName = "";
		//账号_平台:account.getAname() + "_" + account.getThirdPlatform().getAname()
		String accountPlatKey = accountName + "_" + platformName;
		if(thirdAccountReturnMap.containsKey(accountPlatKey)) {
			ThirdAccountDto thirdAccountDto = thirdAccountReturnMap.get(accountPlatKey);
			if(thirdAccountDto.getPaymentrecoverycompany() != null 
					&& thirdAccountDto.getPaymentrecoverycompany().getAid() != null) {
				paymentrecoverycompany = thirdAccountDto.getPaymentrecoverycompany().getAname();
			}
			if(thirdAccountDto.getOrganization() != null && thirdAccountDto.getOrganization().getAid() != null) {
				sellerName = thirdAccountDto.getOrganization().getAname();
				ecommerceMonitor.setMerchantName(sellerName);
			}
		}
		else if(organizationName != null && organizationReturnMap.containsKey(organizationName)) {
			OrganizationDto organizationDto = organizationReturnMap.get(organizationName);
			if(organizationDto.getFinancialCompany() != null && organizationDto.getFinancialCompany().getAid() != null) {
				paymentrecoverycompany = organizationDto.getFinancialCompany().getAname();
			}
			else if(organizationDto.getGoodsFinancialCompany() != null && organizationDto.getGoodsFinancialCompany().getAid() != null) {
				paymentrecoverycompany = organizationDto.getGoodsFinancialCompany().getAname();
			}
		}
		
		/**
		 * 1、平台	平台类型
			Wayfair	TO B
			Overstock	TO B
			The Home Depot	TO B
			Walmart_DSV	TO B
			Walmart_MP	TO C
			Amazon	TO C
		 *
		 * 2、平台	财务公司	提现率
			Wayfair	39Finc	85%
			Overstock	海外广新	42%
			The Home Depot	海外广新	74%
			Walmart_MP	39Finc	85%
			Amazon	39Finc	85%
		 *
		 */
		boolean fincCompanyFlag = StringUtil.containsEqualsIgnoreCase(first39FincList, paymentrecoverycompany);
		boolean gxCompanyFlag = StringUtil.containsEqualsIgnoreCase(secondHWGXList, paymentrecoverycompany);
		
		String orderType = "";
		BigDecimal withdrawalRate = BigDecimal.ZERO;
		if(platformName.equalsIgnoreCase("Wayfair")) {
			orderType = "TO B";
			if(fincCompanyFlag) {
				withdrawalRate = new BigDecimal(0.85);
			}
		}
		else if(platformName.equalsIgnoreCase("Overstock")) {
			orderType = "TO B";
			if(gxCompanyFlag) {
				withdrawalRate = new BigDecimal(0.42);
			}
		}
		else if(platformName.equalsIgnoreCase("The Home Depot") || platformName.equalsIgnoreCase("Home_Depot")) {
			orderType = "TO B";
			if(gxCompanyFlag) {
				withdrawalRate = new BigDecimal(0.74);
			}
		}
		else if(platformName.equalsIgnoreCase("Walmart_DSV")) {
			orderType = "TO B";
		}
		else if(platformName.equalsIgnoreCase("Walmart_MP")) {
			orderType = "TO C";
			if(fincCompanyFlag) {
				withdrawalRate = new BigDecimal(0.85);
			}
		}
		else if(platformName.equalsIgnoreCase("Amazon")) {
			orderType = "TO C";
			if(fincCompanyFlag) {
				withdrawalRate = new BigDecimal(0.85);
			}
		}
		
		ecommerceMonitor.setOrderType(orderType);
		ecommerceMonitor.setFinanceCompany(paymentrecoverycompany);
		ecommerceMonitor.setWithdrawalRate(withdrawalRate);
		
		return ecommerceMonitor;
	}
	
	
	private Map<String, KaPlatformStockForHIB> dealReplMerchantStockMap(List<FourUnitMerchantRepl> replMerchantStockList) {
	
		Map<String, KaPlatformStockForHIB> selfStockMap = new HashMap<>();
		KaPlatformStockForHIB selfStockForHIB = new KaPlatformStockForHIB();
		
		if(replMerchantStockList != null && replMerchantStockList.size() > 0) {
			for (FourUnitMerchantRepl merchantRepl : replMerchantStockList) {
				//20230327 2、导出模板中过滤掉Sub-Category取值为spare part的数据
	        	String category = merchantRepl.getCategory() == null ? "" : merchantRepl.getCategory();
				if(category.equalsIgnoreCase("Spare Parts")){
					continue;
				}
				//海外总库存(含锁定)
	        	BigDecimal inventorySaleableOs = NumberUtil.getBigDecimal(merchantRepl.getInventorySaleableOs());
	        	if(inventorySaleableOs.compareTo(BigDecimal.ZERO) <= 0) {
	        		//库存都没有的数据，不统计
	        		continue;
	        	}
	        	
	        	//平台库存
	        	BigDecimal castleGateStock = NumberUtil.getBigDecimal(merchantRepl.getCastleGate());
	        	BigDecimal fbcStock = NumberUtil.getBigDecimal(merchantRepl.getFba());
	        	BigDecimal fbaStock = NumberUtil.getBigDecimal(merchantRepl.getFbc());
	        	BigDecimal wfsStock = NumberUtil.getBigDecimal(merchantRepl.getWfs());
	        	BigDecimal castleOverstock = NumberUtil.getBigDecimal(merchantRepl.getCastleOverstock());
	        	BigDecimal cpaOverstock = NumberUtil.getBigDecimal(merchantRepl.getCpaOverstock());
	        	BigDecimal wkcOverstock = NumberUtil.getBigDecimal(merchantRepl.getWkcOverstock());
	        	BigDecimal usCaliforniaOverstock = NumberUtil.getBigDecimal(merchantRepl.getUsCaliforniaOverstock());
	        	
	        	BigDecimal platTotalStock = castleGateStock.add(fbaStock).add(fbcStock).add(wfsStock)
	        			.add(castleOverstock).add(cpaOverstock).add(wkcOverstock).add(usCaliforniaOverstock);
	        	
	        	//自营仓库存
	        	BigDecimal saleWareStock = inventorySaleableOs.subtract(platTotalStock);
	        	if(saleWareStock.compareTo(BigDecimal.ZERO) <= 0) {
	        		continue;
	        	}
	        	
	        	//入库商家
	        	String merchant = merchantRepl.getSeller() == null ? "" : merchantRepl.getSeller();
	        	//国家简称
	        	String countryCode = merchantRepl.getCountry() == null ? "" : merchantRepl.getCountry();
	        	
	        	//key:国家简称 + 入库商家 + SKU
	        	String selfStockKey = countryCode + "," + merchant + "," + merchantRepl.getSku();
	        	if(selfStockMap.containsKey(selfStockKey)) {
	        		selfStockForHIB = selfStockMap.get(selfStockKey);
	        		BigDecimal secondQty = saleWareStock.add(NumberUtil.getBigDecimal(selfStockForHIB.getQtyAll()));
	        		selfStockForHIB.setQtyAll(secondQty.intValue());
	        	}else {
	        		selfStockForHIB = new KaPlatformStockForHIB();
	        		selfStockMap.put(selfStockKey, selfStockForHIB);
	        		
	        		selfStockForHIB.setMerchantName(merchant);
	        		selfStockForHIB.setSiteName(countryCode);
	        		selfStockForHIB.setMaterialNumber(merchantRepl.getSku());
	        		selfStockForHIB.setMaterialEngname(merchantRepl.getMeterial());
	        		selfStockForHIB.setFourUnit(merchantRepl.getCompany());
	        		selfStockForHIB.setQtyAll(saleWareStock.intValue());
	        		selfStockForHIB.setCategoryEnglishName(merchantRepl.getCategory());
	        	}
	        	
			}
		}
		return selfStockMap;
	}
	
	
	private Map<String, List<FourUnitSales>> dealReplYesterdayOrder(List<FourUnitSales> allOrderStockList, 
			List<String> fourUnitList, Map<String, KaSkuSeriesMapping> skuSeriesMappingByNameMap,
			Map<String, ErpCloudMaterialAllDto> coreAndSubArticleSkuMap,
			Map<String, OrganizationRelativeMerchandiseDto> hibFourUnitAssociatedMaterialMap) {
		// TODO 获取订单大表昨日订单与成本
		
		List<FourUnitSales> thSkuOrderStockList = new ArrayList<>();
		if(allOrderStockList != null && allOrderStockList.size() > 0) {
			
			for (FourUnitSales fourUnitSales : allOrderStockList) {
				if(fourUnitSales.getFourUnit() != null && fourUnitList.contains(fourUnitSales.getFourUnit())) {
					String fourUnit = fourUnitSales.getFourUnit();
					//处理核心货号
					String productNumber = fourUnitSales.getProductNumber();
					if (productNumber != null && "0000000000000".equals(productNumber)) {
						continue;
					}
					String productName = fourUnitSales.getProductName();
					String categoryEnglishName = fourUnitSales.getCategoryEnglishName();
					//判断是否为核心或号
	                if (coreAndSubArticleSkuMap.containsKey(productNumber)) {
	                    ErpCloudMaterialAllDto erpCloudMaterialAllDto = coreAndSubArticleSkuMap.get(productNumber);
	                    productNumber = erpCloudMaterialAllDto.getCoreSku();
	                    productName = erpCloudMaterialAllDto.getCoreEngname();
	                    //categoryEnglishName = erpCloudMaterialAllDto.getCoreGroupEngname();
	                }
	                
	                //HIB-四字机构关联货号
	                if (hibFourUnitAssociatedMaterialMap.containsKey(productNumber)) {
	                	OrganizationRelativeMerchandiseDto hibORMaterialDto = hibFourUnitAssociatedMaterialMap.get(productNumber);
	                    String hibFourUnit = hibORMaterialDto.getFourOrganization() == null ? "" : hibORMaterialDto.getFourOrganization().getAname();
	                    if (hibFourUnit.equals(fourUnit)) {
	                        //四字机构一致
	                        productNumber = hibORMaterialDto.getAmainMerchandise().getAnumber();
	                        productName = hibORMaterialDto.getAmainMerchandise().getAname();
	                        categoryEnglishName = hibORMaterialDto.getAmainMerchandise().getBaseCategory() == null ? 
	                        		categoryEnglishName : hibORMaterialDto.getAmainMerchandise().getBaseCategory().getAengname(); 
	                    }
	                }
	                
	                if(skuSeriesMappingByNameMap.containsKey(productName)) {
						//匹配SKU系列映射
						KaSkuSeriesMapping kaSkuSeriesMapping = skuSeriesMappingByNameMap.get(productName);
						fourUnitSales.setSuccession(kaSkuSeriesMapping.getSuccession());
					}
	                
	                fourUnitSales.setProductName(productName);
	                fourUnitSales.setProductNumber(productNumber);
	                fourUnitSales.setCategoryEnglishName(categoryEnglishName);
	                
					thSkuOrderStockList.add(fourUnitSales);
				}
			}
		}
		
		//根据Key值分组:平台，账号，站点，SKU编码
		Map<String, List<FourUnitSales>> collectByKeyMap = thSkuOrderStockList.stream().collect(
				Collectors.groupingBy(e -> String.join(",", e.getPlatformName(), e.getPlatformAccountName(), e.getSiteName(), 
						e.getProductNumber())));
		
		
		return collectByKeyMap;
	}
	
	
	private Map<String, List<FourUnitSales>> dealOrderExpressFee(List<FourUnitSales> allOrderExpressFeeList, 
			List<String> fourUnitList, 
			Map<String, ErpCloudMaterialAllDto> coreAndSubArticleSkuMap,
			Map<String, OrganizationRelativeMerchandiseDto> hibFourUnitAssociatedMaterialMap) {
		// TODO 获取订单大表快递费
		
		List<FourUnitSales> thSkuOrderStockList = new ArrayList<>();
		if(allOrderExpressFeeList != null && allOrderExpressFeeList.size() > 0) {
			
			for (FourUnitSales fourUnitSales : allOrderExpressFeeList) {
				if(fourUnitSales.getFourUnit() != null && fourUnitList.contains(fourUnitSales.getFourUnit())) {
					String fourUnit = fourUnitSales.getFourUnit();
					//处理核心货号
					String productNumber = fourUnitSales.getProductNumber();
					if (productNumber != null && "0000000000000".equals(productNumber)) {
						continue;
					}
					String productName = fourUnitSales.getProductName();
					String categoryEnglishName = fourUnitSales.getCategoryEnglishName();
					//判断是否为核心或号
	                if (coreAndSubArticleSkuMap.containsKey(productNumber)) {
	                    ErpCloudMaterialAllDto erpCloudMaterialAllDto = coreAndSubArticleSkuMap.get(productNumber);
	                    productNumber = erpCloudMaterialAllDto.getCoreSku();
	                    productName = erpCloudMaterialAllDto.getCoreEngname();
	                    //categoryEnglishName = erpCloudMaterialAllDto.getCoreGroupEngname();
	                }
	                
	                //HIB-四字机构关联货号
	                if (hibFourUnitAssociatedMaterialMap.containsKey(productNumber)) {
	                	OrganizationRelativeMerchandiseDto hibORMaterialDto = hibFourUnitAssociatedMaterialMap.get(productNumber);
	                    String hibFourUnit = hibORMaterialDto.getFourOrganization() == null ? "" : hibORMaterialDto.getFourOrganization().getAname();
	                    if (hibFourUnit.equals(fourUnit)) {
	                        //四字机构一致
	                        productNumber = hibORMaterialDto.getAmainMerchandise().getAnumber();
	                        productName = hibORMaterialDto.getAmainMerchandise().getAname();
	                        categoryEnglishName = hibORMaterialDto.getAmainMerchandise().getBaseCategory() == null ? 
	                        		categoryEnglishName : hibORMaterialDto.getAmainMerchandise().getBaseCategory().getAengname(); 
	                    }
	                }
	                
	                fourUnitSales.setProductName(productName);
	                fourUnitSales.setProductNumber(productNumber);
	                fourUnitSales.setCategoryEnglishName(categoryEnglishName);
	                
					thSkuOrderStockList.add(fourUnitSales);
				}
			}
		}
		
		//根据Key值分组:平台，账号，站点，SKU编码
		Map<String, List<FourUnitSales>> collectByKeyMap = thSkuOrderStockList.stream().collect(
				Collectors.groupingBy(e -> String.join(",", e.getPlatformName(), e.getPlatformAccountName(), e.getSiteName(), 
						e.getProductNumber())));
		
		
		return collectByKeyMap;
	}
	
	
	private Map<String, List<KaPlatformStockForHIB>> dealPlatformStockData(List<KaPlatformStockForHIB> platformStockList,
			List<String> fourUnitList, Map<String, ErpCloudMaterialAllDto> coreAndSubArticleSkuMap,
			Map<String, OrganizationRelativeMerchandiseDto> hibFourUnitAssociatedMaterialMap) {
		//处理dataaccess-平台库存数据
		List<KaPlatformStockForHIB> pointFourUnitNewList = new ArrayList<>();
		if(platformStockList != null && platformStockList.size() > 0) {
			for (KaPlatformStockForHIB platStockForHIB : platformStockList) {
				if(platStockForHIB.getFourUnit() != null) {
					String fourUnit = platStockForHIB.getFourUnit();
					if(!fourUnitList.contains(fourUnit)) {
						continue;
					}
					String materialNumber = platStockForHIB.getMaterialNumber();
					String materialEngname = platStockForHIB.getMaterialEngname();
					String categoryEnglishName = "";
					//判断是否为核心或号
	                if (coreAndSubArticleSkuMap.containsKey(materialNumber)) {
	                    ErpCloudMaterialAllDto erpCloudMaterialAllDto = coreAndSubArticleSkuMap.get(materialNumber);
	                    materialNumber = erpCloudMaterialAllDto.getCoreSku();
	                    materialEngname = erpCloudMaterialAllDto.getCoreEngname();
	                }
	                
	                //HIB-四字机构关联货号
	                if (hibFourUnitAssociatedMaterialMap.containsKey(materialNumber)) {
	                	OrganizationRelativeMerchandiseDto hibORMaterialDto = hibFourUnitAssociatedMaterialMap.get(materialNumber);
	                    String hibFourUnit = hibORMaterialDto.getFourOrganization() == null ? "" : hibORMaterialDto.getFourOrganization().getAname();
	                    if (hibFourUnit.equals(fourUnit)) {
	                        //四字机构一致
	                    	materialNumber = hibORMaterialDto.getAmainMerchandise().getAnumber();
	                    	materialEngname = hibORMaterialDto.getAmainMerchandise().getAname();
	                        categoryEnglishName = hibORMaterialDto.getAmainMerchandise().getBaseCategory() == null ? 
	                        		categoryEnglishName : hibORMaterialDto.getAmainMerchandise().getBaseCategory().getAengname(); 
	                    }
	                }
	                
	                platStockForHIB.setMaterialNumber(materialNumber);
	                platStockForHIB.setMaterialEngname(materialEngname);
	                platStockForHIB.setCategoryEnglishName(categoryEnglishName);
	                pointFourUnitNewList.add(platStockForHIB);
				}
			}
		}
		
		//根据Key值分组:平台，账号，站点，SKU编码
		Map<String, List<KaPlatformStockForHIB>> platStockByKeyMap = pointFourUnitNewList.stream().collect(
				Collectors.groupingBy(e -> String.join(",", e.getPlatformName(), e.getAccountName(), e.getSiteName(), 
						e.getMaterialNumber())));
		
		return platStockByKeyMap;
		
	}
	
	
	private List<FourUnitMerchantRepl> getReplMerchantStockData(KaReplChartQuery replQuery){
		ObjectMapper mapper = new ObjectMapper();
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.FOUR_REPL_DOWN_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("四字机构补货表库存接口地址 url : {}", url);
		
		List<FourUnitMerchantRepl> thStockList = new ArrayList<>();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				thStockList = mapper.readValue(responseMsg, new TypeReference<List<FourUnitMerchantRepl>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "四字机构补货表库存接口失败："+e); 
		}
		logger.info("thStockList size : {}", thStockList.size());
		
		return thStockList;
	}
	
	
	private List<KaReplSiteSaledetail> getReplSiteSaleData(KaReplChartQuery replQuery){
		ObjectMapper mapper = new ObjectMapper();
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.REPL_SITE_SALE_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("查询补货表-站点销售明细数据接口地址 url : {}", url);
		
		List<KaReplSiteSaledetail> replSiteSaleList = new ArrayList<>();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				replSiteSaleList = mapper.readValue(responseMsg, new TypeReference<List<KaReplSiteSaledetail>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "查询补货表-站点销售明细数据接口失败："+e); 
		}
		logger.info("replSiteSaleList size : {}", replSiteSaleList.size());
		
		return replSiteSaleList;
	}
	
	
	/** 20230423 获取订单大表分录接口数据 */
	private List<FourUnitSales> getOrderDetail(KaReplChartQuery replQuery) {
		
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.FOUR_REPL_ORDER_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		//String url = "http://192.168.1.11:8980/kaOrderDetail/selectMappingCostByPurchaseDate";
		logger.info("查询订单大表数据且匹配成本接口地址 url : {}", url);
		
		
		List<FourUnitSales> allOrderStockList = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				allOrderStockList = mapper.readValue(responseMsg, new TypeReference<List<FourUnitSales>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "查询订单大表数据且匹配成本接口失败："+e); 
		}
		logger.info("allOrderStockList size : {}", allOrderStockList.size());
		
		return allOrderStockList;
	}
	
	/** 20230426 查询订单大表快递费接口数据 */
	private List<FourUnitSales> getOrderExpressFee(KaReplChartQuery replQuery) {
		
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.ORDER_EXPRESS_FEE_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("查询订单大表快递费接口地址 url : {}", url);
		
		replQuery = new KaReplChartQuery();
		Date yesterdayStart = DateUtil.getDayBegin(DateUtil.calculateDate(new Date(), -2));
		Date firstMonthDay = DateUtil.getDayBegin(DateUtil.calculateDate(yesterdayStart, -29));
		replQuery.setAdateExpressFeeFrom(firstMonthDay);
		replQuery.setAdateExpressFeeEnd(yesterdayStart);
		
		List<FourUnitSales> allOrderExpressFeeList = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				allOrderExpressFeeList = mapper.readValue(responseMsg, new TypeReference<List<FourUnitSales>>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "查询订单大表快递费接口失败："+e); 
		}
		logger.info("allOrderExpressFeeList size : {}", allOrderExpressFeeList.size());
		
		return allOrderExpressFeeList;
	}
	
	
	private KaStockForHIBResult getPlatformStockData(KaReplChartQuery replQuery){
		ObjectMapper mapper = new ObjectMapper();
		/** 访问URL */
		PropertiesDto propertiesDto = propertiesApi.selectByAnumber(CommonEnumNumber.PLATFORM_STOCK_NO);
		String url = PrintUtils.formGetValue(propertiesDto.getAproperties()).toString();
		logger.info("查询dataaccess-平台库存数据接口地址 url : {}", url);
		
		//List<KaPlatformStockForHIB> platformStockList = new ArrayList<>();
		KaStockForHIBResult kaStockForHIBResult = new KaStockForHIBResult();
		String jsonParam;
		try {
			jsonParam = JsonUtils.objToJson(replQuery);
			Map<String, String> headermap = new HashMap<>();
			headermap.put("Content-Type", "application/json;charset=utf-8");
			Map<String, Object> resultStockMap = HttpUtil.post(url, jsonParam, headermap);//调用Http请求(传参@RequestBody)
			String code = String.valueOf(resultStockMap.get("code"));//{code=200, responseMsg=[xxx]}
			if("200".equals(code)) {//成功
				String responseMsg = (String)resultStockMap.get("responseMsg");
				//platformStockList = mapper.readValue(responseMsg, new TypeReference<List<KaPlatformStockForHIB>>(){});
				kaStockForHIBResult = mapper.readValue(responseMsg, new TypeReference<KaStockForHIBResult>(){});
			}else {//失败
				logger.info("error : {}", resultStockMap.get("responseMsg")); 
			}
		}catch (Exception e) {
			logger.info("error : {}", "查询dataaccess-平台库存数据接口失败："+e); 
		}
		//logger.info("platformStockList size : {}", platformStockList.size());
		
		return kaStockForHIBResult;
	}
	
	
	private void creatEcommerceMonitorExcel(List<EcommerceMonitor> monitorResultList, SXSSFWorkbook workbook,
			Map<String, Map<String, MerchandiseImageDto>> imgMap) {
		int cellnum = 0;
        Row row = null;
        Cell cell = null;

        Sheet sheet = workbook.createSheet("电商监控表");
        Drawing drawing = sheet.createDrawingPatriarch();
        ClientAnchor anchor = null;
        
        short notColor = 0;
//        Integer row_num = 1;// 行号
        Integer row_num = 0;// 行号
        // 设置标题  设置第一行列中的信息
        CellStyle rowStyle = null;//新样式 设置首行以外的行
//        CellStyle rowStyle = ExcelUtil.getColumnTopStyle(workbook, false, notColor, false, false);//新样式 设置首行以外的行
//        CellRangeAddress c = CellRangeAddress.valueOf("A2:AN2");//新样式 设置筛选
//        sheet.setAutoFilter(c);//新样式 设置筛选
//        
//        sheet.createFreezePane(3, 2, 3, 2); //新样式 锁定首行
//        sheet.setDefaultColumnWidth(10);//新样式 设置列宽
        
        //第一行标题
//        row = sheet.createRow(0);
//       
//        short lightOrangeNo = IndexedColors.LEMON_CHIFFON.getIndex();//浅黄色
//        ExcelUtil.createrow(sheet, row, workbook, 0, "产品数据", 0, 0, 0, 12, lightOrangeNo, true);
//        
//        short lightPurpleBlueNo = IndexedColors.TAN.getIndex();//浅橘色
//        ExcelUtil.createrow(sheet, row, workbook, 13, "库存数据", 0, 0, 13, 16, lightPurpleBlueNo, true);
//        
//        ExcelUtil.createrow(sheet, row, workbook, 17, "销售数据", 0, 0, 17, 24, lightOrangeNo, true);
//        
//        short lightBlueNo = IndexedColors.PALE_BLUE.getIndex();//蓝色
//        ExcelUtil.createrow(sheet, row, workbook, 25, "成本利润数据", 0, 0, 25, 31, lightBlueNo, true);
//        
//        short lightGreenNo = IndexedColors.LIGHT_GREEN.getIndex();//浅绿
//        ExcelUtil.createrow(sheet, row, workbook, 32, "市场竞对数据", 0, 0, 32, 34, lightGreenNo, true);
//        
//        ExcelUtil.createrow(sheet, row, workbook, 35, "运营计划", 0, 0, 35, 37, lightBlueNo, false);
        
        //第二行标题
        List<String> replHead = new ArrayList<String>();//
        replHead.add("");
        replHead.add("序号");
        //产品数据
        replHead.add("HIB SKU");
        replHead.add("款式");
        replHead.add("Sub-Category");
        replHead.add("产品图片");
        replHead.add("集中采购");
        replHead.add("专利");
        replHead.add("Review");
        replHead.add("TOP 20");
        replHead.add("广告");
        replHead.add("自己链接");
        replHead.add("自己链接截图");
        //库存数据
        replHead.add("平台仓库存");
        replHead.add("自营仓库存");
        replHead.add("站点");
        replHead.add("库存量");
        //销售数据
        replHead.add("平台");
        replHead.add("账号");
        replHead.add("财务公司");
        replHead.add("前30天销量");
        replHead.add("前7天销量");
        replHead.add("昨日销量");
        replHead.add("日均销量");
        replHead.add("周转天数");
        //
        //replHead.add("7000运费总利润 (USD)");
        //replHead.add("7000运费利润率");
        //成本利润数据
        replHead.add("FOB成本");
        replHead.add("DDP/DDU成本");
        replHead.add("出库费");
        replHead.add("快递费");
        //replHead.add("自发货成本=出库成本+尾程派送费");
        //replHead.add("批发价");
        replHead.add("提现率");
        replHead.add("利润率");
        replHead.add("零售价");
        //replHead.add("Index=零售价/批发价");
        //replHead.add("定价利润 (盈利/亏损)");
        //市场竞对数据
        replHead.add("竞对链接");
        replHead.add("竞对链接截图");
        replHead.add("竞对零售价");
        //运营计划
        replHead.add("运营计划/备注");
        replHead.add("各平台自定义标签");
        replHead.add("各平台自定义标签");
        replHead.add("四字机构");
        replHead.add("入库商家");
        
        Integer head_size = replHead.size();
        Row headrow = sheet.createRow(row_num++);
        for (cellnum = 0; cellnum < head_size; cellnum++) {
        	cell = headrow.createCell(cellnum);
            cell.setCellValue(replHead.get(cellnum));
            cell.setCellStyle(rowStyle);
        }
        
        String temp = propertiesApi.getSysTempPath();
        logger.info("temp : {}", temp);
        
        int serialNumber = 0;
        if(monitorResultList != null && monitorResultList.size() > 0) {
        	for (EcommerceMonitor monitor : monitorResultList) {
        		//创建行数赋值
                row = sheet.createRow(row_num++);
                row.setHeightInPoints((float) 30.5);//新样式 设置行高 46.5
                cellnum = 0;
                cell = null;
                serialNumber++;
                
                String skuNumber = monitor.getSkuNumber();
                
                //To B
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getOrderType(), rowStyle);
                //序号
                cell = ExcelUtil.setTheReplCell(row, cellnum++, serialNumber, rowStyle);
                /** 产品数据 */
                //HIB SKU
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSkuName(), rowStyle);
                //款式
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSuccession(), rowStyle);
                //Sub-Category
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSkuCategory(), rowStyle);
                //产品图片
//                String httpUrl = null;
//        		String imageUrl = null;
//        		Map<String, MerchandiseImageDto> mainImage = imgMap.get(skuNumber);
//        		if(mainImage != null && mainImage.get("250") != null && mainImage.get("250").getAfile() != null && mainImage.get("250").getAfile().getAhttpurl() != null) {
//					
//        			InputStream inStream = null;
//                    FileOutputStream fileout = null;
//                    FileInputStream fileInput = null;
//                    ByteArrayOutputStream outStream = null;
//                    
//        			httpUrl = mainImage.get("250").getAfile().getAhttpurl();
//					try {
//						imageUrl = httpUrl.substring(0, (httpUrl.lastIndexOf("/") + 1)) + URLEncoder.encode(httpUrl.substring(httpUrl.lastIndexOf("/") + 1), "utf-8");
//						imageUrl = imageUrl.replaceAll("\\\\", "/");
//						
//						String filetype = imageUrl.substring(imageUrl.lastIndexOf("."));
//                        File imagefile = new File(temp + skuNumber + filetype);
//                        if (!imagefile.exists()) {
//                            URL url = new URL(imageUrl);
//                            //打开链接
//                            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//                            //设置请求方式为"GET"
//                            conn.setRequestMethod("GET");
//                            //超时响应时间为5秒  q
//                            conn.setConnectTimeout(5 * 1000);
//                            //通过输入流获取图片数据
//                            inStream = conn.getInputStream();
//
//                            fileout = new FileOutputStream(imagefile);
//                            //创建一个Buffer字符串
//                            byte[] buffer = new byte[1024];
//                            //每次读取的字符串长度，如果为-1，代表全部读取完毕
//                            int len = 0;
//                            //使用一个输入流从buffer里把数据读取出来
//                            while ((len = inStream.read(buffer)) != -1) {
//                                //用输出流往buffer里写入数据，中间参数代表从哪个位置开始读，len代表读取的长度
//                                fileout.write(buffer, 0, len);
//                            }
//                            //关闭输入流
//                            inStream.close();
//                            fileout.close();
//                        }
//                        fileInput = new FileInputStream(imagefile);
//                        outStream = new ByteArrayOutputStream();
//                        //创建一个Buffer字符串
//                        byte[] buffer = new byte[1024];
//                        //每次读取的字符串长度，如果为-1，代表全部读取完毕
//                        int len = 0;
//                        //使用一个输入流从buffer里把数据读取出来
//                        while ((len = fileInput.read(buffer)) != -1) {
//                            //用输出流往buffer里写入数据，中间参数代表从哪个位置开始读，len代表读取的长度
//                            outStream.write(buffer, 0, len);
//                        }
//                        byte[] data = outStream.toByteArray();
//
//                        anchor = drawing.createAnchor(0, 0, 0, 0, cellnum, row_num-1, cellnum + 1, row_num);
//                        anchor.setAnchorType(AnchorType.MOVE_AND_RESIZE);
//                        drawing.createPicture(anchor, workbook.addPicture(data, XSSFWorkbook.PICTURE_TYPE_JPEG));
//						
//						cellnum++;
//					} catch (UnsupportedEncodingException e) {
//						e.printStackTrace();
//						cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSkuPicture(), rowStyle);
//					}catch (Exception e) {
//                        logger.error(e.getMessage(), e);
//                        cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSkuPicture(), rowStyle);
//                    } finally {
//                        //关闭输入流
//                        if (inStream != null) {
//                            try {
//                                inStream.close();
//                            } catch (IOException e) {
//
//                                e.printStackTrace();
//                            }
//                        }
//
//                        if (fileout != null) {
//                            try {
//                                fileout.close();
//                            } catch (IOException e) {
//
//                                e.printStackTrace();
//                            }
//                        }
//
//                        if (fileInput != null) {
//                            try {
//                                fileInput.close();
//                            } catch (IOException e) {
//
//                                e.printStackTrace();
//                            }
//                        }
//
//                        if (outStream != null) {
//                            try {
//                                outStream.close();
//                            } catch (IOException e) {
//
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//        		}else {
//        			cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSkuPicture(), rowStyle);
//        		}
        		
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSkuPicture(), rowStyle);
                //集中采购
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getCentralizedPurchasing(), rowStyle);
                //专利
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPatent(), rowStyle);
                //Review
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getReview(), rowStyle);
                //TOP 20
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getTopTwenty(), rowStyle);
                //广告
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getAdvertisement(), rowStyle);
                //自己链接
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSelfLink(), rowStyle);
                //自己链接截图
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSelfLinkScreenshot(), rowStyle);
                
                /** 库存数据 */
                //平台仓库存
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPlatformStock(), rowStyle);
                //自营仓库存
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSelfStock(), rowStyle);
                //站点
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSiteName(), rowStyle);
                //库存量
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getTotalInventory(), rowStyle);
                
                /** 销售数据 */
                //平台
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPlatformName(), rowStyle);
                //账号
                if(monitor.getPlatformAccountName() != null) {
                	cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPlatformAccountName(), rowStyle);
                }else {
                	cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getMerchantName(), rowStyle);
                }
                //财务公司
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getFinanceCompany(), rowStyle);
                //前30天销量
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSalesThirtyDays(), rowStyle);
                //前7天销量
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSalesSevenDays(), rowStyle);
                //昨日销量
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSalesYesterday(), rowStyle);
                //日均销量
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSalesDayAvr(), rowStyle);
                //周转天数
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getTurnoverDays(), rowStyle);
                //7000运费总利润 (USD)
                //cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSaleYesterdayAmount(), rowStyle);
                //7000运费利润率
                //cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSaleYesterdayProfitRadio(), rowStyle);
                
                /** 成本利润数据 */
                //FOB成本
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getFobCost(), rowStyle);
                //DDP/DDU成本
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getDdpCost(), rowStyle);
                //出库成本
                //cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getOutboundCost(), rowStyle);
                //出库费
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getOutboundFee(), rowStyle);
                //快递费
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getExpressFee(), rowStyle);
                //自发货成本=出库成本+尾程派送费
                //cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getSelfShippingCost(), rowStyle);
                //批发价
                //cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getWholesalePrice(), rowStyle);
                //提现率
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getWithdrawalRate(), rowStyle);
                //利润率
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getProfitMargin(), rowStyle);
                //零售价
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getRetailPrice(), rowStyle);
                //Index=零售价/批发价
                //cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPriceIndexProportion(), rowStyle);
                //定价利润 (盈利/亏损)
                //cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPricingProfit(), rowStyle);
                
                /** 市场竞对数据 */
                //竞对链接
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getCompetitiveLink(), rowStyle);
                //竞对链接截图
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getCompetitiveLinkScreenshot(), rowStyle);
                //竞对零售价
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getCompetitiveRetailPrice(), rowStyle);
                
                /** 运营计划 */
                //运营计划/备注
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getOperationalPlanning(), rowStyle);
                //各平台自定义标签
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPlatformCustomLabelOne(), rowStyle);
                //各平台自定义标签
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getPlatformCustomLabelTwo(), rowStyle);
                
                //四字机构
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getFourUnit(), rowStyle);
                //入库商家
                cell = ExcelUtil.setTheReplCell(row, cellnum++, monitor.getMerchantName(), rowStyle);
			}
        }
	}

	@Override
	public Map<String, Object> downloadColonyTradReport(Context context, HttpServletResponse response, Date reportDate) 
			throws IOException {
		// TODO 20230509 下载集群传统日报表
		Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("flag", true);
        
        UserQueryDto query = new UserQueryDto();
        String currentUserName = PrintUtils.formGetValue(context.getContext(CacheDomain.CURRENT_USER_NAME), "").toString();
        query.setAname(currentUserName);
        List<UserDto> userList = userApi.selectByPageStrictly(query);

        if (ListUtil.isEmpty(userList)) {
            resultMap.put("flag", false);
            resultMap.put("msg", "用户不存在");
            return resultMap;
        }
        
        /** 系统管理员、业务管理员和财务管理员可以下载 */
        //根据用户角色查询管理员权限：
        //业务管理员和系统管理员
  		/*BaseResult baseResult = userRoleApi.checkAdminRoleByUserId(userList.get(0).getAid());
  		//财务和系统管理员
  		BaseResult baseTwoResult = userRoleApi.checkSystemAndFinanceAdminRoleByUserId(userList.get(0).getAid());
  		if (baseResult.isFlag() || baseTwoResult.isFlag()) {*/
        
  			String colonyName = "colony-" + DateUtil.formatDate(new Date(), "yyyy");
  			String fileName = colonyName + ".xlsx";
  			
  			String reportUrl = "https://test.eriabank.net/traditional/report/" +
  					DateUtil.formatDate(new Date(), "yyyy") + "/" + colonyName + ".xlsx";
  			
  			//测试文件地址是否存在
  	        int responseCode = 0;
  	        HttpURLConnection huc = null;
  	        try {
  	            URL url = new URL(reportUrl);
  	            huc = (HttpURLConnection) url.openConnection();
  	            responseCode = huc.getResponseCode();
  	        } finally {
  	            if (huc != null) {
  	                huc.disconnect();
  	            }
  	        }
  	        
  			BaseDataUtil.dowloadFile(response, reportUrl, fileName);
  			response.setHeader("fname", fileName + ".xlsx");
  	        
  	        resultMap.put("responseCode", responseCode);
  	        resultMap.put("reportUrl", reportUrl);
  	        
  		/*}else {
  			resultMap.put("flag", false);
            resultMap.put("msg", "用户没有权限，请联系管理员！");
            return resultMap;
  		}*/
        
        
		return resultMap;
	}

	@Override
	public void downloadEcommerceMonitorSimplified(Context context, HttpServletResponse response) {
		// TODO 20230511 lillian 电商监控表简表
		//参数
		KaReplChartQuery replQuery = new KaReplChartQuery();
		
		List<String> fourUnitList = new ArrayList<>();
		/** 20230505 根据用户账号查询集群下的四字机构 */
		Integer userId = Integer.valueOf((String) context.getContext(CacheDomain.CURRENT_USER_ID));
		UserDto userDto = userApi.selectByPrimaryKey(userId);
        if (userDto == null) {
        	response.setHeader("flag", "0");
            response.setHeader("msg", "用户不存在");
            return;
        }
        FourOrganizationDto fourOrganization = userDto.getFourOrganization();
        if (fourOrganization == null || fourOrganization.getAid() == null) {
        	response.setHeader("flag", "0");
            response.setHeader("msg", "用户没有维护所属机构");
            return;
        }
        //四字机构集合
        fourUnitList = getUserColonyFourUnitList(fourOrganization);
        
        if(fourUnitList == null || fourUnitList.size() == 0) {
        	response.setHeader("flag", "0");
            response.setHeader("msg", "用户没有对应集群信息");
            return;
        }
        
        replQuery.setFourUnitList(fourUnitList);
		replQuery.setAdate(new Date());//取最新的补货表数据
		
		//补货表-站点销售明细数据
		List<KaReplSiteSaledetail> replSiteSaleList = getReplSiteSaleData(replQuery);
		
		//补货表-入库商家明细
		List<FourUnitMerchantRepl> replMerchantStockList = getReplMerchantStockData(replQuery);
		Map<String, EcommerceMonitorSimplified> simplifiedStockMap = dealReplMerchantStockWithSimplified(replMerchantStockList);
		
		/** 处理汇总数据 */
		List<EcommerceMonitorSimplified> ecommerceMonitorList = dealReplSiteDetailWithSimplified(replSiteSaleList, 
				simplifiedStockMap);
		
		//创建excel
		OutputStream os = null;
		String pathName = "电商监控表(简表).xlsx";//
		SXSSFWorkbook workbook = new SXSSFWorkbook(500);
		creatEcommerceMonitorSimplifiedExcel(ecommerceMonitorList, workbook);
		
		try {
            response.reset();
            response.setContentType("application/vnd.ms-excel;charset=UTF-8");
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Disposition", "inline; filename=" + pathName);
            response.setHeader("fname", pathName);
            response.setHeader("flag", "1");
            os = response.getOutputStream();
            workbook.write(os);
            os.flush();
        } catch (Exception e) {
            logger.info("exportExcel", e);
            response.setHeader("flag", "0");
            response.setHeader("msg", e.getMessage());
        } finally {
            try {
                if (os != null)
                    os.close();
                workbook.close();
            } catch (IOException e) {
                logger.info("exportExcel close os or wb", e);
            }
        }
		logger.info("电商监控表表(简表)  Download finish!");
		
		
	}
	
	
	private SXSSFWorkbook creatEcommerceMonitorSimplifiedExcel(List<EcommerceMonitorSimplified> ecommerceMonitorList,
			SXSSFWorkbook workbook) {
		// TODO 电商监控表(简表)excel
		//"在厂成本：FOB价格", 
		String[] titles = {"HIB SKU", "产品图片", "类目", "站点","在产箱数", "在厂箱数", "在途箱数", "海外箱数", 
				"前30天销量（所有平台汇总）", "Amazon前30天销量", "Amazon Vendor前30天销量","Wayfair前30天销量","C-Discount前30天销量",
				"在产成本：工厂价格", "在途成本：DDP（不含税）", "在仓成本：DDP成本+入库费", 
				"Amazon零售价", "Amazon Vendor批发价","Wayfair批发价","C-Discount零售价","Amazon竞品价",
				"四字机构"};
		
		SXSSFSheet sheet = workbook.createSheet("电商监控表(简表)");
		
		SXSSFRow rowIndex = null;
		rowIndex = sheet.createRow(0);
		for (int i = 0; i < titles.length; i++) {
			rowIndex.createCell(i).setCellValue(titles[i]);
		}

		int index = 0;
		int count = 1;
		if (ecommerceMonitorList != null && ecommerceMonitorList.size() > 0) {
			for (EcommerceMonitorSimplified monitorSimplified : ecommerceMonitorList) {
				index = 0;
				rowIndex = sheet.createRow(count);
				
				//HIB SKU
				rowIndex.createCell(index++).setCellValue(monitorSimplified.getSkuName());
				//产品图片
				rowIndex.createCell(index++).setCellValue("");
				//类目
				rowIndex.createCell(index++).setCellValue(monitorSimplified.getSkuCategory());
				//站点
				rowIndex.createCell(index++).setCellValue(monitorSimplified.getSiteName());
				//在产箱数
				if(monitorSimplified.getInProductionStock() != null
						&& monitorSimplified.getInProductionStock().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getInProductionStock().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//在厂箱数
				if(monitorSimplified.getAtTheFactoryStock() != null
						&& monitorSimplified.getAtTheFactoryStock().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getAtTheFactoryStock().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//在途箱数
				if(monitorSimplified.getInTransitStock() != null
						&& monitorSimplified.getInTransitStock().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getInTransitStock().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//海外箱数
				if(monitorSimplified.getOverseasStock() != null
						&& monitorSimplified.getOverseasStock().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getOverseasStock().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//前30天销量（所有平台汇总）
				if(monitorSimplified.getSalesThirtyDays() != null
						&& monitorSimplified.getSalesThirtyDays() != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getSalesThirtyDays());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Amazon前30天销量
				if(monitorSimplified.getAmazonThirtySales() != null
						&& monitorSimplified.getAmazonThirtySales().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getAmazonThirtySales().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Amazon Vendor前30天销量
				if(monitorSimplified.getAmazonVendorThirtySales() != null
						&& monitorSimplified.getAmazonVendorThirtySales().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getAmazonVendorThirtySales().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Wayfair前30天销量
				if(monitorSimplified.getWayfairThirtySales() != null
						&& monitorSimplified.getWayfairThirtySales().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getWayfairThirtySales().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//C-Discount前30天销量
				if(monitorSimplified.getCdiscountThirtySales() != null
						&& monitorSimplified.getCdiscountThirtySales().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getCdiscountThirtySales().intValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//在产成本：工厂价格
				if(monitorSimplified.getInProductionCost() != null
						&& monitorSimplified.getInProductionCost().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getInProductionCost().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//在厂成本：FOB价格
				/*if(monitorSimplified.getFobCost() != null
						&& monitorSimplified.getFobCost().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getFobCost().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}*/
				//在途成本：DDP（不含税）
				if(monitorSimplified.getInTransitDDPCost() != null
						&& monitorSimplified.getInTransitDDPCost().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getInTransitDDPCost().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//在仓成本：DDP成本+入库费
				if(monitorSimplified.getDdpCost() != null
						&& monitorSimplified.getDdpCost().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getDdpCost().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Amazon零售价
				if(monitorSimplified.getAmazonPrice() != null
						&& monitorSimplified.getAmazonPrice().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getAmazonPrice().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Amazon Vendor批发价
				if(monitorSimplified.getAmazonVendorPrice() != null
						&& monitorSimplified.getAmazonVendorPrice().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getAmazonVendorPrice().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Wayfair批发价
				if(monitorSimplified.getWayfairPrice() != null
						&& monitorSimplified.getWayfairPrice().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getWayfairPrice().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//C-Discount零售价
				if(monitorSimplified.getCdiscountPrice() != null
						&& monitorSimplified.getCdiscountPrice().compareTo(BigDecimal.ZERO) != 0) {
					rowIndex.createCell(index++).setCellValue(monitorSimplified.getCdiscountPrice().doubleValue());
				}else {
					rowIndex.createCell(index++).setCellValue("");
				}
				//Amazon竞品价
				rowIndex.createCell(index++).setCellValue("");
				//四字机构
				rowIndex.createCell(index++).setCellValue(monitorSimplified.getFourUnit());
				
				
				
				count++;
			}
		}
		
		return workbook;
	}

	private Map<String, EcommerceMonitorSimplified> dealReplMerchantStockWithSimplified(
			List<FourUnitMerchantRepl> replMerchantStockList) {
		//TODO 处理入库商家补货表库存给到电商监控表简表
		Map<String, EcommerceMonitorSimplified> simplifiedStockMap = new HashMap<>();
		EcommerceMonitorSimplified stockSimplified = new EcommerceMonitorSimplified();
		
		if(replMerchantStockList != null && replMerchantStockList.size() > 0) {
			for (FourUnitMerchantRepl merchantRepl : replMerchantStockList) {
				//20230327 2、导出模板中过滤掉Sub-Category取值为spare part的数据
	        	String category = merchantRepl.getCategory() == null ? "" : merchantRepl.getCategory();
				if(category.equalsIgnoreCase("Spare Parts")){
					continue;
				}
				//站点
				String siteName = merchantRepl.getCountry() == null ? "" : merchantRepl.getCountry();
				//海外总库存(含锁定)
	        	BigDecimal inventorySaleableOs = NumberUtil.getBigDecimal(merchantRepl.getInventorySaleableOs());
	        	
	        	//ComingSoon:在产箱数
	        	BigDecimal comingSoon = NumberUtil.getBigDecimal(merchantRepl.getComingSoon());
	        	//Ship Now：在厂箱数
	        	BigDecimal shipNow = NumberUtil.getBigDecimal(merchantRepl.getShipNow());
	        	//On the Way：在途箱数
	        	BigDecimal onTheWay = NumberUtil.getBigDecimal(merchantRepl.getOnTheWay());
	        	BigDecimal onTheWayTwo = NumberUtil.getBigDecimal(merchantRepl.getOnTheWayTwo());
	        	BigDecimal inTransitStock = onTheWay.add(onTheWayTwo);
	        	
	        	BigDecimal totalStock = inventorySaleableOs.add(comingSoon).add(shipNow).add(onTheWay).add(onTheWayTwo);
	        	if(totalStock.compareTo(BigDecimal.ZERO) <= 0) {
	        		//库存都没有的数据，不统计
	        		continue;
	        	}
	        	
	        	String skuNumber = merchantRepl.getSku();
	        	String fourUnit = merchantRepl.getCompany();
	        	//key:SKU + 四字机构 + 站点
	        	String skuFourKey = skuNumber + "," + fourUnit + "," + siteName;
	        	if(simplifiedStockMap.containsKey(skuFourKey)) {
	        		stockSimplified = simplifiedStockMap.get(skuFourKey);
	        		stockSimplified.setInProductionStock(comingSoon.add(stockSimplified.getInProductionStock()));//在产箱数
	        		stockSimplified.setAtTheFactoryStock(shipNow.add(stockSimplified.getAtTheFactoryStock()));//在厂箱数
	        		stockSimplified.setInTransitStock(inTransitStock.add(stockSimplified.getInTransitStock()));//在途箱数
	        		stockSimplified.setOverseasStock(inventorySaleableOs.add(stockSimplified.getOverseasStock()));//海外箱数
	        	}else {
	        		stockSimplified = new EcommerceMonitorSimplified();
	        		simplifiedStockMap.put(skuFourKey, stockSimplified);
	        		
	        		stockSimplified.setFourUnit(fourUnit);
	        		stockSimplified.setSkuNumber(skuNumber);
	        		stockSimplified.setSiteName(siteName);
	        		stockSimplified.setSkuName(merchantRepl.getMeterial());
	        		stockSimplified.setSkuCategory(merchantRepl.getCategory());
	        		stockSimplified.setInProductionStock(comingSoon);//在产箱数
	        		stockSimplified.setAtTheFactoryStock(shipNow);//在厂箱数
	        		stockSimplified.setInTransitStock(inTransitStock);//在途箱数
	        		stockSimplified.setOverseasStock(inventorySaleableOs);//海外箱数
	        	}
	        	
			}
			
			
		}
		return simplifiedStockMap;
	}
	
	
	private List<EcommerceMonitorSimplified> dealReplSiteDetailWithSimplified(
			List<KaReplSiteSaledetail> replSiteSaleList,
			Map<String, EcommerceMonitorSimplified> simplifiedStockMap) {
		//TODO 处理补货表-站点销售明细获取结果集
		
		//<物料+四字机构 + 站点，结果集>
		Map<String, List<KaReplSiteSaledetail>> siteDetailBySkuMap = new HashMap<>();
		List<KaReplSiteSaledetail> skuDetailList = new ArrayList<>();
		
		//<物料+四字机构+平台，结果集>
		Map<String, List<KaReplSiteSaledetail>> siteDetailBySkuPlatMap = new HashMap<>();
		List<KaReplSiteSaledetail> skuPlatDetailList = new ArrayList<>();
		
		Map<String, BigDecimal> fobCostMap = new HashMap<>();
		Map<String, BigDecimal> ddpCostMap = new HashMap<>();
		
		if(replSiteSaleList != null && replSiteSaleList.size() > 0) {
			for (KaReplSiteSaledetail kaReplSiteSaledetail : replSiteSaleList) {
				//20230327 2、导出模板中过滤掉Sub-Category取值为spare part的数据
	        	String category = kaReplSiteSaledetail.getCategory() == null ? "" : kaReplSiteSaledetail.getCategory();
				if(category.equalsIgnoreCase("Spare Parts")){
					continue;
				}
				
				String fourUnit = kaReplSiteSaledetail.getFourUnit();
				String skuNumber = kaReplSiteSaledetail.getSku() == null ? "" : kaReplSiteSaledetail.getSku();
				//String siteName = kaReplSiteSaledetail.getCountry() == null ? "" : kaReplSiteSaledetail.getCountry();
				String siteName = kaReplSiteSaledetail.getSiteName() == null ? "" : kaReplSiteSaledetail.getSiteName();
				//key:SKU + 四字机构 + 站点
				String skuFourKey = skuNumber + "," + fourUnit + "," + siteName;
				//<物料+四字机构，结果集>
				if(siteDetailBySkuMap.containsKey(skuFourKey)) {
					skuDetailList = siteDetailBySkuMap.get(skuFourKey);
				}else {
					skuDetailList = new ArrayList<>();
					siteDetailBySkuMap.put(skuFourKey, skuDetailList);
				}
				skuDetailList.add(kaReplSiteSaledetail);
				
				//<物料+四字机构+平台，结果集>
				String platformName = kaReplSiteSaledetail.getPlatformName() == null ? "" : kaReplSiteSaledetail.getPlatformName();
				String skuplatKey = skuNumber + "," + fourUnit + "," + platformName + "," + siteName;
				if(siteDetailBySkuPlatMap.containsKey(skuplatKey)) {
					skuPlatDetailList = siteDetailBySkuPlatMap.get(skuplatKey);
				}else {
					skuPlatDetailList = new ArrayList<>();
					siteDetailBySkuPlatMap.put(skuplatKey, skuPlatDetailList);
				}
				skuPlatDetailList.add(kaReplSiteSaledetail);
				
				
				if(kaReplSiteSaledetail.getUploadFOBCost() != null) {
					if(!fobCostMap.containsKey(skuFourKey)) {
						fobCostMap.put(skuFourKey, kaReplSiteSaledetail.getUploadFOBCost());
					}
				}
				if(kaReplSiteSaledetail.getUploadDDPCost() != null) {
					if(!ddpCostMap.containsKey(skuplatKey)) {
						ddpCostMap.put(skuFourKey, kaReplSiteSaledetail.getUploadDDPCost());
					}
				}else {
					if(!ddpCostMap.containsKey(skuplatKey)) {
						ddpCostMap.put(skuFourKey, kaReplSiteSaledetail.getUploadCost());
					}
				}
			}
		}
		
		//结果集
		List<EcommerceMonitorSimplified> ecommerceMonitorList = new ArrayList<>();
		Set<String> skuFourKeySet = new HashSet<>();
		
		if(siteDetailBySkuMap != null && siteDetailBySkuMap.size() > 0) {
			for (Entry<String, List<KaReplSiteSaledetail>> skuEntry : siteDetailBySkuMap.entrySet()) {
				String skuFourKey = skuEntry.getKey();//skuFourKey = skuNumber + "," + fourUnit + "," + siteName;
				String[] split = skuFourKey.split(",");
				String siteName = split[2];
				List<KaReplSiteSaledetail> skuSiteDetailList = skuEntry.getValue();
				
				BigDecimal salesThirtyDays = BigDecimal.ZERO;
				if(skuSiteDetailList != null && skuSiteDetailList.size() > 0) {
					
					//月销量(过去30天)
					long sumSalesThirtyDays = skuSiteDetailList.stream().collect(
							Collectors.summarizingInt(KaReplSiteSaledetail::getSalesThirtyDays)).getSum();
					salesThirtyDays = NumberUtil.getBigDecimal(sumSalesThirtyDays);
				
					KaReplSiteSaledetail kaReplSiteSaledetail = skuSiteDetailList.get(0);
					String fourUnit = kaReplSiteSaledetail.getFourUnit();//四字机构
					String skuNumber = kaReplSiteSaledetail.getSku() == null ? "" : kaReplSiteSaledetail.getSku();
					
					EcommerceMonitorSimplified ecommerceMonitor = new EcommerceMonitorSimplified();
					ecommerceMonitorList.add(ecommerceMonitor);
					ecommerceMonitor.setFourUnit(fourUnit);
					ecommerceMonitor.setSiteName(kaReplSiteSaledetail.getSiteName());
					ecommerceMonitor.setSkuNumber(kaReplSiteSaledetail.getSku());
					ecommerceMonitor.setSkuName(kaReplSiteSaledetail.getMeterial());
					ecommerceMonitor.setSkuCategory(kaReplSiteSaledetail.getCategory());
					ecommerceMonitor.setSalesThirtyDays(salesThirtyDays.intValue());
					
					if(fobCostMap.containsKey(skuFourKey)) {
						ecommerceMonitor.setFobCost(fobCostMap.get(skuFourKey));
					}
					if(ddpCostMap.containsKey(skuFourKey)) {
						ecommerceMonitor.setDdpCost(ddpCostMap.get(skuFourKey));
					}
					
					//Amazon前30天销量
					String platform = "Amazon";
					getReplSitePlatPrice(ecommerceMonitor, skuNumber, fourUnit, platform, siteName, siteDetailBySkuPlatMap);
					
					//Amazon Vendor前30天销量
					platform = "amazon vendor";
					getReplSitePlatPrice(ecommerceMonitor, skuNumber, fourUnit, platform, siteName, siteDetailBySkuPlatMap);
					
					//Wayfair前30天销量
					platform = "Wayfair";
					getReplSitePlatPrice(ecommerceMonitor, skuNumber, fourUnit, platform, siteName, siteDetailBySkuPlatMap);
					
					//C-Discount前30天销量
					platform = "Cdiscount";
					getReplSitePlatPrice(ecommerceMonitor, skuNumber, fourUnit, platform, siteName, siteDetailBySkuPlatMap);
					
					//库存
					if(simplifiedStockMap.containsKey(skuFourKey)) {
						skuFourKeySet.add(skuFourKey);
						EcommerceMonitorSimplified monitorSimplified = simplifiedStockMap.get(skuFourKey);
						ecommerceMonitor.setInProductionStock(monitorSimplified.getInProductionStock());//在产箱数
						ecommerceMonitor.setAtTheFactoryStock(monitorSimplified.getAtTheFactoryStock());//在厂箱数
						ecommerceMonitor.setInTransitStock(monitorSimplified.getInTransitStock());//在途箱数
						ecommerceMonitor.setOverseasStock(monitorSimplified.getOverseasStock());//海外箱数
					}
				}
				
				
			}
		}
		
		//库存补漏
		if(simplifiedStockMap != null && simplifiedStockMap.size() > 0) {
			for (Entry<String, EcommerceMonitorSimplified> simplifiedEntry : simplifiedStockMap.entrySet()) {
				String skuFourKey = simplifiedEntry.getKey();
				if(!skuFourKeySet.contains(skuFourKey)) {
					EcommerceMonitorSimplified ecommerceMonitorSimplified = simplifiedEntry.getValue();
					ecommerceMonitorList.add(ecommerceMonitorSimplified);
					if(fobCostMap.containsKey(skuFourKey)) {
						ecommerceMonitorSimplified.setFobCost(fobCostMap.get(skuFourKey));
					}
					if(ddpCostMap.containsKey(skuFourKey)) {
						ecommerceMonitorSimplified.setDdpCost(ddpCostMap.get(skuFourKey));
					}
				}
			}
		}
		
		return ecommerceMonitorList;
	}
	
	//获取补货表-站点销售明细的平台销量和售价
	private EcommerceMonitorSimplified getReplSitePlatPrice(EcommerceMonitorSimplified ecommerceMonitor, 
			String skuNumber, String fourUnit, String platform, String siteName,
			Map<String, List<KaReplSiteSaledetail>> siteDetailBySkuPlatMap) {
		
		if(siteDetailBySkuPlatMap != null && siteDetailBySkuPlatMap.size() > 0) {
			String skuplatKey = skuNumber + "," + fourUnit + "," + platform + "," + siteName;
			if(siteDetailBySkuPlatMap.containsKey(skuplatKey)) {
				List<KaReplSiteSaledetail> list = siteDetailBySkuPlatMap.get(skuplatKey);
				
				//销售金额（美元）（过去30天）
				BigDecimal sumSalesAmountThirty = list.stream()
	                    .filter(e -> e.getSalesAmountThirty() != null)
	                    .map(KaReplSiteSaledetail::getSalesAmountThirty)
	                    .reduce(BigDecimal.ZERO, BigDecimal::add);
				
				//月销量(过去30天)
				long sumSalesThirtyDays = list.stream().collect(
						Collectors.summarizingInt(KaReplSiteSaledetail::getSalesThirtyDays)).getSum();
				BigDecimal salesThirtyDays = NumberUtil.getBigDecimal(sumSalesThirtyDays);
				
				//平台售价
				BigDecimal retailPrice = BigDecimal.ZERO;
				if(salesThirtyDays.compareTo(BigDecimal.ZERO) != 0) {
					retailPrice = sumSalesAmountThirty.divide(salesThirtyDays, 4, BigDecimal.ROUND_HALF_UP);
				}
				
				if(platform != null) {
					if(platform.equalsIgnoreCase("Amazon")) {
						ecommerceMonitor.setAmazonThirtySales(salesThirtyDays);
						ecommerceMonitor.setAmazonPrice(retailPrice);
					}
					else if (platform.equalsIgnoreCase("amazon vendor")) {
						ecommerceMonitor.setAmazonVendorThirtySales(salesThirtyDays);
						ecommerceMonitor.setAmazonVendorPrice(retailPrice);
					}
					else if (platform.equalsIgnoreCase("Wayfair")) {
						ecommerceMonitor.setWayfairThirtySales(salesThirtyDays);
						ecommerceMonitor.setWayfairPrice(retailPrice);
					}
					else if (platform.equalsIgnoreCase("Cdiscount")) {
						ecommerceMonitor.setCdiscountThirtySales(salesThirtyDays);
						ecommerceMonitor.setCdiscountPrice(retailPrice);
					}
				}
				
			}
		}
		
		return ecommerceMonitor;
	}
	
}