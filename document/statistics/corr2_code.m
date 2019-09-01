corrs=zeros(10,1);
for i = 1:10
    corrs(i)=corr2(table2array(total(:, 5+ (i-1)*8)), table2array(total(:, 92)));
end

for i = 1:10
    corrs(i)=corr2(table2array(total(:, 5+ (i-1)*8)), table2array(total(:, 11+ (i-1)*8)));
end
corrs