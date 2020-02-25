require 'csv'
require 'thread'
require 'tiny_tds'
require './secrets'
require './db'

#print "\naccount id?: "
# accid = $stdin.gets.chomp()
accid = 5459312
accid = 296299

db = Queryer.new accid
db.populate_contacts

batch_size = 200
num_batches = db.contacts.count % batch_size > 0 ? db.contacts.count / batch_size + 1 : db.contacts.count
batches = []

while batches.count < num_batches do 
    batch = []
    batch_size.times do 
        staged = db.contacts.pop
        batch << staged unless staged.nil?
    end
    batches << batch
end

rows = []
batches.each_with_index do |batch, i| 
    ids, records = [], []

    # collecting the ids from this batch and creating empty account object for each one
    batch.each do |c| 
        record = { 
            'id' => c['customerid'],
            'emailaddress' => c['emailaddress'],
            'firstname' => c['fname'],
            'lastname' => c['lname'],
            'customerstatus' => c['customerstatus'],
            'streetaddress1' => c['streetaddress1'],
            'streetaddress2' => c['streetaddress2'],
            'city' => c['city'],
            'state' => c['state'],
            'zip' => c['zip'],
            'dateofbirth' => c['dateofbirth']
        }
        db.fields.each { |field| record.store(field, '') }
        db.chars.each { |char| record.store(char, '') }
        pp c if record['id'].to_s == '28218171'
        pp record if record['id'].to_s == '28218171'
        records << record
        ids << c['customerid']
    end

    # appending field names and values to account (by id)
    raw_fields = db.get_batch_field_values(ids)
    raw_fields.each do |fv| 
        record = records.detect { |a| a['id'] == fv['recipientid'] }
        record[fv['fielddescription']] = fv['value'] if !record.nil? && fv['recipientid'] == record['id']
    end

    # appending characteristics to account (by id)
    raw_chars = db.get_batch_customer_characteristics(ids)
    raw_chars.each do |cv| 
        record = records.detect { |a| a['id'] == cv['customerid'] }
        record[cv['characteristicname']] = cv['characteristicname'] if !record.nil? && cv['customerid'] == record['id']
    end

    records.each { |record| rows << record }
    puts "batch #{i + 1} of #{num_batches} complete (#{batch_size} per batch)"
end
    
if rows.count > 0
    puts "writing file..."
    filename = "mv_export_#{db.accid}.csv"
    CSV.open(filename, "wb") do |csv|
        csv << rows.first.keys
        rows.each do |h| 
            csv << h.values 
        end
    end
   puts "done: #{filename}"
else
    puts "empty resultset; no file generated"
end
