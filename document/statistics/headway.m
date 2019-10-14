headw = table2array(temporal(:,1));
PTSTdiff = table2array(temporal(:,2));
PTATdiff = table2array(temporal(:,3));

[COR, pval] = corr(PTSTdiff, headw)

[COR3, pval3] = corr(PTATdiff, headw)