# razpravljalnica
Implementation of razpravljalnica - Reddit like chat backend in golang

!!!!!!! MessageId mora biti sekvenčen int64 !!!!!!

* metoda post, bi lahko sprejela ali topicId, ki je int64 ali pa topicName, ki je string....če daš zdej note ime topica ti vedno vrže v topic 0
* v message morava dodati poleg user_id še username, pa mogoče topicName, da se bo lahko lepše izpisovalo
* implement getSubscriptionNode v control_plane.go