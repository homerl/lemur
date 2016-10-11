@ali
Feature: ali data mover
        As a Lustre administrator
        I want to configure a ali data mover
        In order to migrate Lustre file data to and from a ali bucket.

Background:
        Given I am the root user
        And I have a Lustre filesystem
        And the HSM coordinator is enabled
        When I configure the HSM Agent
        And I configure the ali data mover
        And I start the HSM Agent
        Then the HSM Agent should be running
        And the ali data mover should be running

Scenario: 
        When I archive folder1/cancer
        Then folder1/cancer should be marked as archived
        And the data for folder1/cancer should be archived
	
	And I have released folder1/cancer	

	When I restore folder1/cancer
        Then the data for folder1/cancer should be restored
	
        When I remove folder1/cancer
        Then folder1/cancer should be marked as unmanaged
        And the data for folder1/cancer should be removed


