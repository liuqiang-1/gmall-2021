package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.atguigu.gmallpublisher.bean.*;


@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHours(String date) {
        HashMap<String, Long> resultMap = new HashMap<>();
        List<Map> maps = dauMapper.selectDauTotalHourMap(date);
        for (Map map : maps) {
            resultMap.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return resultMap;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
//        create_hour 1      sum_amount  123
        List<Map> maps = orderMapper.selectOrderAmountHourMap(date);
        HashMap<String, Double> hourmap = new HashMap<>();
        for (Map map : maps) {
            hourmap.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }
        return hourmap;
    }

    @Override
    public Map getSaleDetail(String date, int startpage, int size, String keyword) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //?????? ??????
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //????????????
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);

        //????????????
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);

        //?????? = ???????????? -1 ???* ????????????
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);
        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_SALE_DETAIL_INDEX + "0625").addType("_doc").build();

        SearchResult result = jestClient.execute(search);
        //1.??????????????????
        Long total = result.getTotal();
        //2.??????????????????
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            detail.add(hit.source);
        }
        //TODO ??????list????????????????????????  Option
        ArrayList<Option> ageOptions = new ArrayList<>();
        TermsAggregation groupby_age = result.getAggregations().getTermsAggregation("groupby_user_age");
        Long low20Count = 0L;
        Long up30Count = 0L;
        for (TermsAggregation.Entry bucket : groupby_age.getBuckets()) {
            if (Integer.parseInt(bucket.getKey()) < 20) {
                low20Count += bucket.getCount();
            } else if (Integer.parseInt(bucket.getKey()) > 30) {
                up30Count += bucket.getCount();
            }
        }
        //????????????20??????????????????
        double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;

        //????????????20??????????????????
        double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;
        //20???30??????????????????
        double up20ToLow30Ratio = 100 - low20Count - up30Count;
        Option low20Opt = new Option("20????????????", low20Ratio);
        Option up20to30lowOpt = new Option("20??????30???????????????", up20ToLow30Ratio);
        Option up30Opt = new Option("30????????????", up30Ratio);
        ageOptions.add(low20Opt);
        ageOptions.add(up20to30lowOpt);
        ageOptions.add(up30Opt);

        //TODO ??????list????????????????????????  Option
        ArrayList<Option> genderOptions = new ArrayList<>();
        TermsAggregation groupby_gender = result.getAggregations().getTermsAggregation("groupby_user_gender");
        Long maleCount = 0L;
        Long femaleCount = 0L;

        for (TermsAggregation.Entry bucket : groupby_gender.getBuckets()) {
            if ("???".equals(bucket.getKey())) {
                maleCount += bucket.getCount();
            } else if ("???".equals(bucket.getKey())) {
                femaleCount += bucket.getCount();
            }
            //??? || ???  ????????????
            double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
            double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
            Option maleOpt = new Option("???????????????", maleRatio);
            Option femaleOpt = new Option("???????????????", femaleRatio);
            genderOptions.add(maleOpt);
            genderOptions.add(femaleOpt);

        }

        //??????list???????????????stat
        ArrayList<Stat> stats = new ArrayList<>();
        Stat ageStat = new Stat("??????????????????", ageOptions);
        Stat genderStat = new Stat("??????????????????", genderOptions);
        stats.add(ageStat);
        stats.add(genderStat);

        //???????????????????????????Map
        HashMap<String, Object> finalMap = new HashMap<>();
        finalMap.put("total", total);
        finalMap.put("stat", stats);
        finalMap.put("detail", detail);
        return finalMap;
    }
}