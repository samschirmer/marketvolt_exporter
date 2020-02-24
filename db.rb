class Queryer
    attr_accessor :db, :accid, :contacts, :fields, :chars

    def initialize(id)
        creds = Creds.get
        @db = TinyTds::Client.new username: creds[:user], password: creds[:pass], host: creds[:host], database: creds[:db]
        @accid = id
        @contacts = get_contacts
        @fields = fetch_all_field_names(id)
        @chars = fetch_all_char_names(id)
    end

    def fetch_all_field_names(id)
        names = []
        sql = "select fieldid, fielddescription from customerfields2 where accountid = #{id}"
        res = @db.execute(sql)
        res.each { |r| names << r['fielddescription'] }
        return names
    end

    def fetch_all_char_names(id)
        names = []
        sql = "select characteristicid, characteristicname from characteristics where deletedfromaccount = 0 and accountid = #{id} order by characteristicname"
        res = @db.execute(sql)
        res.each { |r| names << r['characteristicname'] }
        return names
    end

    def get_field_values(id)
        sql = 
            "select cf.fielddescription, d.value
            from (select fieldid, fielddescription from customerfields2 where accountid = #{@accid} and fielddescription not in ('First Name','Last Name','Street Address 1','Street Address 2','City','State','Zip','Date of Birth','Phone Number','Preferred Format','Email Address')) as cf
            left join (select fieldid, value from recipientfielddata where recipientid = #{id}) as d on d.fieldid = cf.fieldid"
        @db.execute(sql)
    end

    def get_customer_characteristics(id)
        sql = 
            "select ch.characteristicid, ch.characteristicname, cc.customerid
            from (select characteristicid, characteristicname from characteristics where deletedfromaccount = 0 and accountid = #{@accid}) as ch
            left join (select characteristicid, customerid from customercharacteristics where customerid = #{id}) as cc on cc.characteristicid = ch.characteristicid
            order by ch.characteristicname"
        @db.execute(sql)
    end

    def get_batch_field_values(ids)
            #from (select fieldid, fielddescription from customerfields2 where accountid = #{@accid} and fielddescription not in ('First Name','Last Name','Street Address 1','Street Address 2','City','State','Zip','Date of Birth','Phone Number','Preferred Format','Email Address')) as cf
        sql = 
            "select cf.fielddescription, d.value, d.recipientid
            from (select fieldid, fielddescription from customerfields2 where accountid = #{@accid} and fielddescription not in ('First Name','Last Name','Street Address 1','Street Address 2','City','State','Zip','Date of Birth','Phone Number','Preferred Format','Email Address')) as cf
            left join (select fieldid, value, recipientid from recipientfielddata where recipientid in (#{ids.join(',')})) as d on d.fieldid = cf.fieldid"
        @db.execute(sql)
    end

    def get_batch_customer_characteristics(ids)
        sql = 
            "select ch.characteristicid, ch.characteristicname, cc.customerid
            from (select characteristicid, characteristicname from characteristics where deletedfromaccount = 0 and accountid = #{@accid}) as ch
            left join (select characteristicid, customerid from customercharacteristics where customerid in (#{ids.join(',')})) as cc on cc.characteristicid = ch.characteristicid
            order by ch.characteristicname"
        @db.execute(sql)
    end


    def populate_contacts
        @contacts = get_contacts
    end

    def get_contacts
        contacts = []
        sql = "
            select customerid, emailaddress, fname, lname, customerstatus, streetaddress1, streetaddress2, city, state, zip, dateofbirth 
            from customers where isnull(deletedfromaccount,0) != 1 and accountid = #{@accid}"
        base_contacts = @db.execute(sql)
        base_contacts.each do |c|
            contacts << c 
        end
        return contacts
    end

end