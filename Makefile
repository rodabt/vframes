.PHONY: link docs

link:
	ln -s /home/rabt/devel/vframes /home/rabt/.vmodules/rodabt/vframes

docs:
	VDOC_SORT=false v doc -comments -color -f html -m vframes -o docs -readme -inline-assets