package com.impactupgrade.integration.sfdc;

import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.partner.sobject.SObject;
import org.junit.Assert;
import org.junit.Test;

public class SFDCPartnerAPIClientTest {

  private static final SFDCPartnerAPIClient CLIENT = new SFDCPartnerAPIClient("", "", ""){};

  @Test
  public void testToPartner() {
    Contact contact = new Contact();
    contact.setId("c123");
    contact.setAccountId("a123");
    contact.setFirstName("Brett");
    contact.setLastName("Meyer");
    contact.setFieldsToNull(new String[]{"Phone", "Email"});

    SObject partnerSObject = CLIENT.toPartner(contact);

    Assert.assertEquals("c123", partnerSObject.getId());
    Assert.assertEquals("a123", partnerSObject.getSObjectField("AccountId"));
    Assert.assertEquals("Brett", partnerSObject.getSObjectField("FirstName"));
    Assert.assertEquals("Meyer", partnerSObject.getSObjectField("LastName"));
    Assert.assertArrayEquals(new String[]{"Phone", "Email"}, partnerSObject.getFieldsToNull());
  }

  @Test
  public void testToEnterprise() {
    SObject sObject = new SObject("Contact");
    sObject.setId("c123");
    sObject.setSObjectField("AccountId", "a123");
    sObject.setSObjectField("FirstName", "Brett");
    sObject.setSObjectField("LastName", "Meyer");
    sObject.setFieldsToNull(new String[]{"Phone", "Email"});

    SObject subSObject = new SObject("Account");
    subSObject.setId("a123");
    subSObject.setSObjectField("Name", "Meyer Household");
    sObject.setSObjectField("Account", subSObject);

    Contact contact = CLIENT.toEnterprise(Contact.class, sObject);

    Assert.assertEquals("c123", contact.getId());
    Assert.assertEquals("a123", contact.getAccountId());
    Assert.assertEquals("Brett", contact.getFirstName());
    Assert.assertEquals("Meyer", contact.getLastName());
    Assert.assertArrayEquals(new String[]{"Phone", "Email"}, contact.getFieldsToNull());

    Assert.assertNotNull(contact.getAccount());
    Assert.assertEquals("a123", contact.getAccount().getId());
    Assert.assertEquals("Meyer Household", contact.getAccount().getName());
  }
}
