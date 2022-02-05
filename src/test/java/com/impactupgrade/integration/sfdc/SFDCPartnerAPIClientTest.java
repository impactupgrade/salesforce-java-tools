package com.impactupgrade.integration.sfdc;

import com.sforce.soap.enterprise.sobject.Account;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.partner.sobject.SObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SFDCPartnerAPIClientTest {

  private static final SFDCPartnerAPIClient CLIENT = new SFDCPartnerAPIClient("", "", "", 200){};

  @Test
  public void testToPartner() {
    Contact contact = new Contact();
    contact.setId("c123");
    contact.setAccountId("a123");
    contact.setAge__c(35.0);
    contact.setEmail_Opt_Out__c(true);
    contact.setFirstName("Brett");
    contact.setLastName("Meyer");
    contact.setFieldsToNull(new String[]{"Phone", "Email"});

    Account account = new Account();
    account.setId("a123");
    account.setName("Meyer Household");
    contact.setAccount(account);

    SObject partnerSObject = CLIENT.toPartner(contact);

    assertEquals("c123", partnerSObject.getId());
    assertEquals(35.0, partnerSObject.getSObjectField("Age__c"));
    assertEquals(true, partnerSObject.getSObjectField("Email_Opt_Out__c"));
    assertEquals("Brett", partnerSObject.getSObjectField("FirstName"));
    assertEquals("Meyer", partnerSObject.getSObjectField("LastName"));
    assertArrayEquals(new String[]{"Phone", "Email"}, partnerSObject.getFieldsToNull());
    assertEquals("a123", partnerSObject.getSObjectField("AccountId"));
    assertNotNull(partnerSObject.getSObjectField("Account"));
    assertEquals("a123", ((SObject) partnerSObject.getSObjectField("Account")).getSObjectField("Id"));
    assertEquals("Meyer Household", ((SObject) partnerSObject.getSObjectField("Account")).getSObjectField("Name"));
  }

  @Test
  public void testToEnterprise() {
    SObject sObject = new SObject("Contact");
    sObject.setId("c123");
    sObject.setSObjectField("AccountId", "a123");
    // query result fields are always strings, so test for conversion
    sObject.setSObjectField("Age__c", "35.0");
    sObject.setSObjectField("Birthdate", "1985-10-30");
    // query result fields are always strings, so test for conversion
    sObject.setSObjectField("Email_Opt_Out__c", "true");
    sObject.setSObjectField("FirstName", "Brett");
    sObject.setSObjectField("LastName", "Meyer");
    sObject.setFieldsToNull(new String[]{"Phone", "Email"});

    SObject subSObject = new SObject("Account");
    subSObject.setId("a123");
    subSObject.setSObjectField("Name", "Meyer Household");
    sObject.setSObjectField("Account", subSObject);

    Contact contact = CLIENT.toEnterprise(Contact.class, sObject);

    assertEquals("c123", contact.getId());
    assertEquals("a123", contact.getAccountId());
    assertEquals(Double.valueOf(35.0), contact.getAge__c());
    assertEquals(1985, contact.getBirthdate().get(Calendar.YEAR));
    // reminder: Java time is horrible, and month starts at 0, because reasons
    assertEquals(10, contact.getBirthdate().get(Calendar.MONTH) + 1);
    assertEquals(30, contact.getBirthdate().get(Calendar.DATE));
    assertEquals(true, contact.getEmail_Opt_Out__c());
    assertEquals("Brett", contact.getFirstName());
    assertEquals("Meyer", contact.getLastName());
    assertArrayEquals(new String[]{"Phone", "Email"}, contact.getFieldsToNull());

    assertNotNull(contact.getAccount());
    assertEquals("a123", contact.getAccount().getId());
    assertEquals("Meyer Household", contact.getAccount().getName());
  }

  @Test
  public void testToEnterpriseDefaultValues() {
    SObject partnerSObject = new SObject("Contact");
    Contact contact = CLIENT.toEnterprise(Contact.class, partnerSObject);
    assertEquals(false, contact.getEmail_Opt_Out__c());

    // then convert it back to SObject and make sure the default isn't carried over
    partnerSObject = CLIENT.toPartner(contact);
    assertNull(partnerSObject.getSObjectField("Email_Opt_Out__c"));
  }

  @Test
  public void testToEnterpriseWithFirstLetterLowercase() {
    SObject partnerSObject = new SObject("Contact");
    // query result fields are always strings, so test for conversion
    partnerSObject.setSObjectField("isSalesforceSilly__c", "true");
    Contact contact = CLIENT.toEnterprise(Contact.class, partnerSObject);
    assertEquals(true, contact.getIsSalesforceSilly__c());

    partnerSObject = CLIENT.toPartner(contact);
    assertEquals(true, partnerSObject.getSObjectField("isSalesforceSilly__c"));
  }
  
  @Test
  public void testToMap() {
    SObject user = new SObject("User");
    user.setField("Email", "someone@some.org");

    SObject account = new SObject("Account");
    account.setField("Name", "Meyer Household");
    account.setField("Owner", user);
    
    SObject contact = new SObject("Contact");
    contact.setField("FirstName", "Brett");
    contact.setField("LastName", "Meyer");
    contact.setField("Account", account);
    
    SObject opportunity = new SObject("Opportunity");
    opportunity.setField("Name", "Meyer Donation");
    opportunity.setField("Amount", 5.0);
    opportunity.setField("Contact", contact);

    Map<String, Object> opportunityMap = CLIENT.toMap(opportunity);

    assertEquals("Meyer Donation", opportunityMap.get("Name"));
    assertEquals(5.0, opportunityMap.get("Amount"));
    assertEquals("Brett", opportunityMap.get("Contact.FirstName"));
    assertEquals("Meyer", opportunityMap.get("Contact.LastName"));
    assertEquals("Meyer Household", opportunityMap.get("Contact.Account.Name"));
    assertEquals("someone@some.org", opportunityMap.get("Contact.Account.Owner.Email"));
  }
}
