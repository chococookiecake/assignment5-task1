#assignment5-1	
##设计思路   
	第1个mapper是用于得到<股票代码,1>这样的键值对  
	第1个reducer是用于得到<股票代码,出现次数>这样的键值对  
	第2个mapper是用于得到<出现次数,股票代码>这样的键值对  
	第2个reducer是用于得到<排名>：<股票代码>，<次数>  

##程序运行结果
1:MS,726  
2:MRK,704  
3:QQQ,693  
4:BABA,689  
5:EWU,681  
6:GILD,663  
7:JNJ,663  
8:MU,659  
9:NVDA,655  
10:VZ,648  
11:KO,643  
12:QCOM,636  
13:M,635  
14:NFLX,635  
15:EBAY,621  
16:DAL,605  
17:WFC,582  
18:BBRY,581  
19:ORCL,575  
20:FDX,573  
21:BMY,563  
22:AA,561  
23:JCP,559  
24:EWP,553  
25:NOK,532  
26:EWJ,526  
27:GLD,513  
28:EWI,510  
29:LMT,509  
30:CHK,508  
31:GPRO,508  
32:HD,506  
33:TWX,506  
34:GPS,502  
35:P,501  
......  




##提交作业运⾏成功的WEB⻚⾯截图  

![image](https://github.com/user-attachments/assets/704ca0c9-2163-42fe-840b-83f53741d64c)  


 
##性能、扩展性等⽅⾯存在的不⾜和可能的改进之处	

由于我没有使用诸如TotalOrderPartitioner这样的partitioner，所以我没法做到多个reducer的全局排序，所以我限制reducer数量为1，这肯定是非常非常不利于scale up的，是一个很大的不足
