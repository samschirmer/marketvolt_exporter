require 'csv'
require 'tiny_tds'
require './secrets'
require './db'

print "\naccount id?: "
accid = $stdin.gets.chomp()
db = Queryer.new accid

rows = []
db.contacts.each_with_index do |row, i|
    field_values = db.get_field_values(row[:customerid])
    field_values.each(symbolize_keys: true) { |fv| row.store(fv[:fielddescription], fv[:value]) }

    char_values = db.get_customer_characteristics(row[:customerid])
    char_values.each(symbolize_keys: true) { |cv| row.store(cv[:characteristicname], cv[:customerid].nil? ? "" : cv[:characteristicname]) }

    rows << row
    puts "#{i} of #{db.contacts.count} complete"
end

if rows.count > 0
    filename = "mv_export_#{db.accid}.csv"
    CSV.open(filename, "wb") do |csv|
        csv << rows.first.keys
        rows.each { |h| csv << h.values }
    end
   puts "done: #{filename}"
else
    puts "empty resultset; no file generated"
end
