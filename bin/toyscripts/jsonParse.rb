
require "json"

begin

#  doc = "{ insert: \"products\", documents: [ { name: \"Product 001\", _id: ObjectId('590a2613fe4f7e486803c726'), internal: { approximatePriceUSD: 10 }, category: { _id: \"product00001\", parent: \"Category\", ancestors: [] }, price: { amount: 10, currency: \"USD\" }, pictures: [ \"https://images-na.ssl-images-amazon.com/images/I/71BjuhV5ceL._SL1477_.jpg\" ], __v: 0 } ], ordered: false, writeConcern: { w: 1 } }"



 # _doc_ = " \"snp_research.snps\", ordered: true, documents: [ { has_sig: false, rsid: \"rs149589\", loci: [], chr: \"21\", _id: ObjectId('591603c845733c03cf2dace4') } ] }"

#  _doc_ = " \"snp_research.snps\", \"ordered\": true, \"documents\": [ { \"has_sig\": false, \"rsid\": \"rs11089\", \"loci\": [ { \"gene\": \"MCM3AP\", \"mrna_acc\": \"NM_003906.3\", \"class\": \"reference\" }, { \"gene\": \"MCM3AP\", \"mrna_acc\": \"NM_003906.3\", \"class\": \"synonymous-codon\" }, { \"gene\": \", \"mrna_acc\": \"NR_002776.3\", \"class\": \"intron-variant\" } ], \"chr\": \"21\", \"_id\": ObjectId('591603c845733c03cf2dad3d') } ] }"


  _doc_ = "{ \"gene\": \"}"

  doc =  '{ "key" :'+ _doc_
  doc.gsub!("\"",'"')
  doc.gsub!(/(\w+):/,'"\1":')
  doc.gsub!('""','"')
  doc.gsub!('"://',"://")
  doc.gsub!(/ObjectId\((\'\w+\')\)/,'"ObjectId(\1)"')
  doc.gsub!(/:\s*"\s*,/,':"",')
  puts "-- Documents --"
  puts doc

  begin
    flag = true
    puts "-- Hash --"
    puts JSON.parse(doc)
  rescue JSON::ParserError => e
    p e
    flag = false
  end
  if(flag)then
    puts "TRUE"
  else
    puts "FALSE"
  end
end
