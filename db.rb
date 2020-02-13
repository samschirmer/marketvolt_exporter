class Queryer
    attr_accessor :db, :accid, :contacts

    def initialize(id)
        creds = Creds.get
        @db = TinyTds::Client.new username: creds[:user], password: creds[:pass], host: creds[:host], database: creds[:db]
        @accid = id
        @contacts = get_contacts
    end

    def get_field_values(id)
        sql = 
            "select cf.fielddescription, d.value
            from (select fieldid, fielddescription from customerfields2 where accountid = #{@accid}) as cf
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

private

    def get_contacts
        contacts = []
        sql = "
            select customerid, emailaddress, fname, lname, customerstatus, streetaddress1, streetaddress2, city, state, zip, dateofbirth 
            from customers where isnull(deletedfromaccount,0) != 1 and accountid = #{@accid}"
        base_contacts = @db.execute(sql)
        base_contacts.each(symbolize_keys: true) do |c|
            contacts << c 
        end
        return contacts
    end

end