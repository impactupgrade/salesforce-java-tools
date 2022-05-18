# salesforce-java-tools

Working with the Salesforce Parnter API and Enterprise API can be a **pain**. Although we're old school and appreciate some of the benefits of strongly-typed SOAP contracts, it's easy to wind up with a solution that's brittle and not resilient.

salesforce-java-tools is the backbone of all our deep Salesforce integrations within the [Nonprofit Nucleus](https://www.impactupgrade.com/nonprofit-nucleus/) engine. In particular, <a href="https://github.com/impactupgrade/salesforce-java-tools/blob/master/src/main/java/com/impactupgrade/integration/sfdc/SFDCPartnerAPIClient.java">SFDCPartnerAPIClient</a> supports all the bells-and-whistles and hardening we require: mapping between Partner API and Enterprise API objects, batched actions, retries, paging, session/connection caching, and DRY helper methods.

## Impact Upgrade

[Impact Upgrade](https://www.impactupgrade.com) is a tech and operations consulting company that solely partners with nonprofits and for-good businesses. We make the complex simple, focusing on back-office operations and making big ideas a reality. Don't fight a multiple-front battle! You know your mission. We know tech.

## Usage

Add the following Maven dependency:

```xml
<dependency>
    <groupId>com.impactupgrade.integration</groupId>
    <artifactId>salesforce-java-tools</artifactId>
    <version>0.3.2-SNAPSHOT</version>
</dependency>
```

Code example:

TODO

## How to Deploy a Snapshot

1. Add the following to ~/.m2/settings.xml
```xml
<server>
  <id>ossrh</id>
  <username>USERNAME</username>
  <password>PASSWORD</password>
</server>
```
2. mvn clean deploy

## How to Deploy a Release

1. Add the following to ~/.m2/settings.xml
```xml
<server>
  <id>ossrh</id>
  <username>USERNAME</username>
  <password>PASSWORD</password>
</server>
```
2. mvn versions:set -DnewVersion=1.2.3.Final
3. git add .
4. git commit -m "1.2.3.Final release"
5. git tag 1.2.3.Final
6. mvn clean deploy -P release
7. mvn versions:set -DnewVersion=1.2.4-SNAPSHOT
8. git add .
9. git commit -m "1.2.4-SNAPSHOT"
10. git push origin master 1.2.3.Final

## License

Licensed under the GNU AFFERO GENERAL PUBLIC LICENSE V3 (GNU AGPLv3). See agpl-3.0.txt for more information.
